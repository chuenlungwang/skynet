local driver = require "socketdriver"
local skynet = require "skynet"
local skynet_core = require "skynet.core"
local assert = assert

local socket = {}	-- api

--[[ 缓存节点池, 当有套接字消息到来时用于装载其中的消息. 其元素是缓存节点数组的 userdata,
具体描述参考 lua-socket.c 中的 lpushbuffer 中的描述. ]]
local buffer_pool = {}	-- store all message buffer object

--[[ 保存所有套接字相关信息的表, 其键为套接字的 id, 值为套接字信息, 字段参考 connect 函数,
此表能够在回收内存时关闭所有套接字. ]]
local socket_pool = setmetatable( -- store all socket object
	{},
	{ __gc = function(p)
		for id,v in pairs(p) do
			driver.close(id)
			-- don't need clear v.buffer, because buffer pool will be free at the end
			p[id] = nil
		end
	end
	}
)

--[[用于处理套接字消息的函数表, 其键是套接字事件类型, 定义在 skynet_socket.h 中. 当 skynet_socket.c 中的
skynet_socket_poll 函数被调用时将会携带相应的套接字信息调用此表中的函数. ]]
local socket_message = {}

--[[ 唤醒一个套接字的协程, 此套接字的线程之前由于未完成的动作而调用 suspend 阻塞自己的协程.
之所以需要这样一套机制的原因在于套接字操作是异步的, 发起命令和命令的处理并不在一个过程中. ]]
local function wakeup(s)
	local co = s.co
	if co then
		s.co = nil
		skynet.wakeup(co)
	end
end

--[[ 阻塞套接字自身的协程, 当等待协程被唤醒之后的第一件事情就是检查并唤醒关闭协程, 因为关闭协程会在存在阻塞协程时,
等待阻塞协程完成自己的工作. ]]
local function suspend(s)
	assert(not s.co)
	s.co = coroutine.running()
	skynet.wait(s.co)
	-- wakeup closing corouting every time suspend,
	-- because socket.close() will wait last socket buffer operation before clear the buffer.
	if s.closing then
		skynet.wakeup(s.closing)
	end
end

-- read skynet_socket.h for these macro
-- SKYNET_SOCKET_TYPE_DATA = 1
--[[ 当收到 TCP 套接字消息时将触发此函数, 函数首先将数据放入到缓存列表中, 并在存在等待读取协程的情况下, 唤醒它们.
等待读取的协程有两种, 一种是按字节数读取, 一种按照行分隔符来读取, 对于后一种还会检查是否超出了缓存限制, 如果是的话
将关闭套接字. 还有一种特殊的读取操作, 要等到套接字关闭才会读取全部的数据, 其 read_required 为 true, 同样检查缓存限制. ]]
socket_message[1] = function(id, size, data)
	local s = socket_pool[id]
	if s == nil then
		skynet.error("socket: drop package from " .. id)
		driver.drop(data, size)
		return
	end

	local sz = driver.push(s.buffer, buffer_pool, data, size)
	local rr = s.read_required
	local rrt = type(rr)
	if rrt == "number" then
		-- read size
		if sz >= rr then
			s.read_required = nil
			wakeup(s)
		end
	else
		if s.buffer_limit and sz > s.buffer_limit then
			skynet.error(string.format("socket buffer overflow: fd=%d size=%d", id , sz))
			driver.clear(s.buffer,buffer_pool)
			driver.close(id)
			return
		end
		if rrt == "string" then
			-- read line
			if driver.readline(s.buffer,nil,rr) then
				s.read_required = nil
				wakeup(s)
			end
		end
	end
end

-- SKYNET_SOCKET_TYPE_CONNECT = 2
--[[ 报告连接成功, 并唤醒发起连接的协程. 连接的方式有多种: 主动发起连接、接受连接、开启端口侦听、操作系统套接字绑定
或者套接字转移. 具体描述参考 socket_sever.c 中返回 SOCKET_OPEN 的函数. ]]
socket_message[2] = function(id, _ , addr)
	local s = socket_pool[id]
	if s == nil then
		return
	end
	-- log remote addr
	s.connected = true
	wakeup(s)
end

-- SKYNET_SOCKET_TYPE_CLOSE = 3
--[[ 报告套接字的关闭, 套接字可能在发生错误或者主动关闭的情况下, 套接字关闭后将唤醒等待的协程. ]]
socket_message[3] = function(id)
	local s = socket_pool[id]
	if s == nil then
		return
	end
	s.connected = false
	wakeup(s)
end

-- SKYNET_SOCKET_TYPE_ACCEPT = 4
--[[ 报告侦听套接字上连接上来的客户端套接字. 并调用预先在此套接字上设置的回调函数. ]]
socket_message[4] = function(id, newid, addr)
	local s = socket_pool[id]
	if s == nil then
		driver.close(newid)
		return
	end
	s.callback(newid, addr)
end

-- SKYNET_SOCKET_TYPE_ERROR = 5
--[[ 报告套接字中的错误, 将写入错误日志并关闭这个套接字, 如果是正在连接的套接字, 将返回这个错误到 connecting 字段中.
最后唤醒等待执行的协程. 除非是调用 socket.close 主动关闭套接字, 否则不会将套接字信息表从套接字池中清除, 这是为了读操作不出错.
此函数最终需要手动清除套接字池中的套接字信息表, 调用 socket.invalid 可以完成清除任务. ]]
socket_message[5] = function(id, _, err)
	local s = socket_pool[id]
	if s == nil then
		skynet.error("socket: error on unknown", id, err)
		return
	end
	if s.connected then
		skynet.error("socket: error on", id, err)
	elseif s.connecting then
		s.connecting = err
	end
	s.connected = false
	driver.shutdown(id)

	wakeup(s)
end

-- SKYNET_SOCKET_TYPE_UDP = 6
--[[ 报告接收到 UDP 套接字信息. 如果套接字不存在将释放此消息, 否则将此消息转化为 string 类型,
释放掉原消息并调用回调处理函数. ]]
socket_message[6] = function(id, size, data, address)
	local s = socket_pool[id]
	if s == nil or s.callback == nil then
		skynet.error("socket: drop udp package from " .. id)
		driver.drop(data, size)
		return
	end
	local str = skynet.tostring(data, size)
	skynet_core.trash(data, size)
	s.callback(str, address)
end

--[[ 默认的警告函数, 当需要发送的缓冲大小大于某一个阈值(1MB)时将收到警告消息.
默认行为是当消息积累到比原来打 64K 时记录警告消息.

参数: id 是当前套接字的 id; size 是此套接字等待发送的数据缓冲大小, 单位是 KB; ]]
local function default_warning(id, size)
	local s = socket_pool[id]
		local last = s.warningsize or 0
		if last + 64 < size then	-- if size increase 64K
			s.warningsize = size
			skynet.error(string.format("WARNING: %d K bytes need to send out (fd = %d)", size, id))
		end
		s.warningsize = size
end

-- SKYNET_SOCKET_TYPE_WARNING
--[[ 当接收到警告消息时调用警告函数, 警告函数可以是自定义的也可以是系统默认的. ]]
socket_message[7] = function(id, size)
	local s = socket_pool[id]
	if s then
		local warning = s.warning or default_warning
		warning(id, size)
	end
end

--[[ 在服务中注册套接字消息处理函数, 当套接字消息到来时将首先调用 unpack 解包, 并调用 dispatch 分发此消息. ]]
skynet.register_protocol {
	name = "socket",
	id = skynet.PTYPE_SOCKET,	-- PTYPE_SOCKET = 6
	unpack = driver.unpack,
	dispatch = function (_, _, t, ...)
		socket_message[t](...)
	end
}

--[[ 当进行主动发起连接(open)、接受连接(start)、开启端口侦听(start)、操作系统套接字绑定(bind)
或者套接字转移(start) , 都需要生成一个新的套接字信息表. 这个函数只针对 TCP 类型的套接字.
对于开启端口侦听来说是不会接收客户端数据的, 因而就没有缓存列表 buffer. 当生成好了套接字信息表之后将阻塞直到
包括连接成功, 或者连接出错了.

参数: id 是发起连接生成的套接字 id; func 目前仅在开启端口侦听的情况下才会提供, 用于在接收客户端连接时进行回调处理.
返回: 连接成功时返回套接字 id, 失败时返回 nil 和错误原因. ]]
local function connect(id, func)
	local newbuffer
	if func == nil then
		newbuffer = driver.buffer()
	end
	local s = {
		id = id,
		buffer = newbuffer,
		connected = false,
		connecting = true,
		read_required = false,
		co = false,
		callback = func,
		protocol = "TCP",
	}
	assert(not socket_pool[id], "socket is not closed")
	socket_pool[id] = s
	suspend(s)
	local err = s.connecting
	s.connecting = nil
	if s.connected then
		return id
	else
		socket_pool[id] = nil
		return nil, err
	end
end

--[[ 主动发起套接字连接, 协程将会阻塞等待连接成功或失败.

参数: string addr 是主机地址, 可以是 [ipv6]:port 或者 ipv4:port 形式, 此时将不需要用提供 port 参数;
     int/string port 如果 addr 仅仅包含 ip 地址, 则 port 将提供一个端口;
	 
返回: 如果连接成功将返回套接字 id, 如果失败将返回 nil 和失败原因. ]]
function socket.open(addr, port)
	local id = driver.connect(addr,port)
	return connect(id)
end

--[[ 将操作系统中的文件描述符绑定到 skynet 系统中去, 并返回一个对应的套接字 id.
参数: int os_fd 是系统文件描述符;
返回: 如果绑定成功将返回这个套接字 id, 如果失败将返回 nil 和失败原因. ]]
function socket.bind(os_fd)
	local id = driver.bind(os_fd)
	return connect(id)
end

--[[ 将标准输入绑定为 skynet 系统中的套接字. 并返回套接字 id. ]]
function socket.stdin()
	return socket.bind(0)
end

--[[ 调用 listen 和报告接收连接的套接字都是不会上报任何 I/O 事件的, 需要主动开启上报 I/O 事件
并为它们生成套接字信息表. 对于开启端口侦听的调用还需要提供一个回调函数, 当报告接收连接时将会调用这个回调函数.
除此以外还有套接字转移时需要调用这个函数, 将一个套接字从别的服务转移到当前服务, 这之后消息将会上报到此服务.

参数: id 是已经存在的套接字 id; function func 仅在打开端口侦听的情况下才会提供, 用于接收连接时进行回调;
返回: 如果调用成功将返回套接字 id, 如果失败将返回 nil 和失败原因. ]]
function socket.start(id, func)
	driver.start(id)
	return connect(id, func)
end

--[[ 在关闭套接字时清理掉套接字中的缓存列表的信息, 如果套接字已经连接成功, 同时将调用回调函数释放套接字本身.
参数: int id 是套接字 id; function func 是回调函数, 仅当套接字还处于连接状态时调用回调函数 ]]
local function close_fd(id, func)
	local s = socket_pool[id]
	if s then
		if s.buffer then
			driver.clear(s.buffer,buffer_pool)
		end
		if s.connected then
			func(id)
		end
	end
end

--[[ 立即关闭套接字, 函数先处理掉套接字的缓存列表, 再发起强制关闭套接字命令. 此函数只能处理已经添加到了套接字池中的套接字.
如果是没有加入套接字池中的套接字, 比如调用 socket.listen 得到的套接字, 应该调用 socket.close_fd 函数.
此函数最终需要手动清除套接字池中的套接字信息表, 调用 socket.invalid 可以完成清除任务. ]]
function socket.shutdown(id)
	close_fd(id, driver.shutdown)
end

--[[ 关闭套接字, 但没有从套接字池中清除此套接字信息表. 此函数只能针对没有添加到套接字池中的套接字调用. ]]
function socket.close_fd(id)
	assert(socket_pool[id] == nil,"Use socket.close instead")
	driver.close(id)
end

--[[ 主动关闭套接字, 如果套接字还有未完成的读取操作, 关闭操作将等待读取完成再清除缓存列表, 并从套接字池中清除套接字信息表.
此函数只能处理已经添加到了套接字池中的套接字. 并且不能用于 __gc 函数中, 因为 skynet.wait 不会返回到 __gc 函数中. ]]
function socket.close(id)
	local s = socket_pool[id]
	if s == nil then
		return
	end
	if s.connected then
		driver.close(id)
		-- notice: call socket.close in __gc should be carefully,
		-- because skynet.wait never return in __gc, so driver.clear may not be called
		if s.co then
			-- reading this socket on another coroutine, so don't shutdown (clear the buffer) immediately
			-- wait reading coroutine read the buffer.
			assert(not s.closing)
			s.closing = coroutine.running()
			skynet.wait(s.closing)
		else
			suspend(s)
		end
		s.connected = false
	end
	close_fd(id)	-- clear the buffer (already close fd)
	assert(s.lock == nil or next(s.lock) == nil)
	socket_pool[id] = nil
end

--[[ 从套接字中读取指定长度的消息并以字符串形式返回. 如果没有提供 sz 参数, 那么表示读取所有的数据.
函数会在无法满足读取时检查套接字的状态, 如果套接字已经关闭了将返回 false 外加缓存列表中的所有数据,
如果套接字还在连接, 那么将阻塞等待数据的到来, 并继续读取数据, 如果依旧无法读取到数据, 将返回 false 和缓存中的所有数据.

参数: int id 是需要读取数据的套接字 id , 它应该事先添加到套接字池中; int/nil sz 当提供时表示读取的字节数, 不提供表示读取所有;
返回: 成功时返回读取到的数据, 无法满足读取请求时返回 false 和缓存列表中的所有数据. ]]
function socket.read(id, sz)
	local s = socket_pool[id]
	assert(s)
	if sz == nil then
		-- read some bytes
		local ret = driver.readall(s.buffer, buffer_pool)
		if ret ~= "" then
			return ret
		end

		if not s.connected then
			return false, ret
		end
		assert(not s.read_required)
		s.read_required = 0
		suspend(s)
		ret = driver.readall(s.buffer, buffer_pool)
		if ret ~= "" then
			return ret
		else
			return false, ret
		end
	end

	local ret = driver.pop(s.buffer, buffer_pool, sz)
	if ret then
		return ret
	end
	if not s.connected then
		return false, driver.readall(s.buffer, buffer_pool)
	end

	assert(not s.read_required)
	s.read_required = sz
	suspend(s)
	ret = driver.pop(s.buffer, buffer_pool, sz)
	if ret then
		return ret
	else
		return false, driver.readall(s.buffer, buffer_pool)
	end
end

--[[ 读取套接字中的所有数据, 此函数会等待套接字关闭并读取套接字缓存列表中的所有数据.
在没有数据可读的情况下将返回 false 或者空字符串, 否则返回读取到的所有数据. ]]
function socket.readall(id)
	local s = socket_pool[id]
	assert(s)
	if not s.connected then
		local r = driver.readall(s.buffer, buffer_pool)
		return r ~= "" and r
	end
	assert(not s.read_required)
	s.read_required = true
	suspend(s)
	assert(s.connected == false)
	return driver.readall(s.buffer, buffer_pool)
end

--[[ 从套接字中读取一行, 行分隔符为 sep , 若没有提供此参数则为默认的 \n 分隔符. 函数会等待套接字的缓存列表中有足够的一行的数据,
并读取这一行数据. 如果最终在没有一行数据时套接字却关闭了, 将返回 false 以及缓存列表中的所有数据. ]]
function socket.readline(id, sep)
	sep = sep or "\n"
	local s = socket_pool[id]
	assert(s)
	local ret = driver.readline(s.buffer, buffer_pool, sep)
	if ret then
		return ret
	end
	if not s.connected then
		return false, driver.readall(s.buffer, buffer_pool)
	end
	assert(not s.read_required)
	s.read_required = sep
	suspend(s)
	if s.connected then
		return driver.readline(s.buffer, buffer_pool, sep)
	else
		return false, driver.readall(s.buffer, buffer_pool)
	end
end

--[[ 在套接字上阻塞, 并等待套接字上到来的事件(数据达到、关闭事件、出错事件)唤醒此协程.
函数最终返回套接字是否还处于连接状态. 如果套接字已经关闭了, 将不再等待就直接返回 false . ]]
function socket.block(id)
	local s = socket_pool[id]
	if not s or not s.connected then
		return false
	end
	assert(not s.read_required)
	s.read_required = 0
	suspend(s)
	return s.connected
end

socket.write = assert(driver.send)
socket.lwrite = assert(driver.lsend)
socket.header = assert(driver.header)

--[[ 从套接字池中将此套接字清除掉, 只有当关闭掉了此套接字并清除了它的缓存列表时才可以调用此函数. ]]
function socket.invalid(id)
	return socket_pool[id] == nil
end

--[[ 对主机和端口进行侦听. 其中如果 port 不提供, 则 host 必须是冒号分割地址和端口的形式, 也可以只提供端口号,
而 host 是空字符串, 此时将侦听 0.0.0.0 . backlog 是未完成连接的请求的队列大小, 如果不提供则使用系统默认值. ]]
function socket.listen(host, port, backlog)
	if port == nil then
		host, port = string.match(host, "([^:]+):(.+)$")
		port = tonumber(port)
	end
	return driver.listen(host, port, backlog)
end

--[[ 锁住一个套接字, 当执行必须串行化的操作时需要先锁住套接字, 并在操作完之后解锁套接字.
虽然服务的执行是单线程的, 但是执行过程依然是无序的, 随时都有可能交出执行权限. 如果已经有别的协程锁住了套接字,
当前协程将在锁集中等待被唤醒. ]]
function socket.lock(id)
	local s = socket_pool[id]
	assert(s)
	local lock_set = s.lock
	if not lock_set then
		lock_set = {}
		s.lock = lock_set
	end
	if #lock_set == 0 then
		lock_set[1] = true
	else
		local co = coroutine.running()
		table.insert(lock_set, co)
		skynet.wait(co)
	end
end

--[[ 解锁一个套接字. 当执行完串行化操作之后需要调用此函数. 如果还有其它的协程在等待锁住套接字, 将唤醒下一个加锁协程. ]]
function socket.unlock(id)
	local s = socket_pool[id]
	assert(s)
	local lock_set = assert(s.lock)
	table.remove(lock_set,1)
	local co = lock_set[1]
	if co then
		skynet.wakeup(co)
	end
end

-- abandon use to forward socket id to other service
-- you must call socket.start(id) later in other service
--[[ 从套接字池中清除套接字 id 的套接字信息表和它的缓存列表. 这个函数是为将套接字 id 转移到别的服务中去. ]]
function socket.abandon(id)
	local s = socket_pool[id]
	if s and s.buffer then
		driver.clear(s.buffer,buffer_pool)
	end
	socket_pool[id] = nil
end

--[[ 给套接字 id 设置缓存大小限制. ]]
function socket.limit(id, limit)
	local s = assert(socket_pool[id])
	s.buffer_limit = limit
end

---------------------- UDP

--[[ 生成一个 UDP 套接字信息表, 默认是连接成功的, 提供的回调函数 cb 将在接收到 UDP 包时调用. ]]
local function create_udp_object(id, cb)
	assert(not socket_pool[id], "socket is not closed")
	socket_pool[id] = {
		id = id,
		connected = true,
		protocol = "UDP",
		callback = cb,
	}
end

--[[ 生成一个 UDP 套接字, 其中回调函数 callback 将在接收到 UDP 包时调用. 主机地址 host 和端口号 port 都可以不提供,
此时将不绑定地址. host 是主机地址, 可以是 [ipv6]:port 或者 ipv4:port 形式, 此时将不需要用提供 port 参数;
如果 addr 仅仅包含 ip 地址, 则 port 将提供一个端口; 

返回: 生成的 UDP 套接字 id; ]]
function socket.udp(callback, host, port)
	local id = driver.udp(host, port)
	create_udp_object(id, callback)
	return id
end

--[[ 发起 UDP 连接, 其实质是将由主机和端口标识的地址关联到套接字中去, 需要注意套接字类型要与地址类型一致.
调用成功之后就可以通过 socket.write 和 socket.lwrite 发送数据了. addr 和 port 的格式与 socket.upd 中的地址格式一样.
最后的回调函数 callback 将替换原来的 UDP 消息回调函数, callback 参数可以不提供. ]]
function socket.udp_connect(id, addr, port, callback)
	local obj = socket_pool[id]
	if obj then
		assert(obj.protocol == "UDP")
		if callback then
			obj.callback = callback
		end
	else
		create_udp_object(id, callback)
	end
	driver.udp_connect(id, addr, port)
end

socket.sendto = assert(driver.udp_send)
socket.udp_address = assert(driver.udp_address)

--[[ 给套接字设置警告函数, 警告函数必须是接收两个参数, 第一个参数数套接字 id, 第二个参数是以 KB 为单位的数据大小. ]]
function socket.warning(id, callback)
	local obj = socket_pool[id]
	assert(obj)
	obj.warning = callback
end

return socket
