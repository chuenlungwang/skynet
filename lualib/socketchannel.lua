local skynet = require "skynet"
local socket = require "socket"
local socketdriver = require "socketdriver"

-- channel support auto reconnect , and capture socket error in request/response transaction
-- { host = "", port = , auth = function(so) , response = function(so) session, data }

local socket_channel = {}
local channel = {}
local channel_socket = {}

--[[ 通道的元表, 调用 socket_channel.channel 方法得到通道, 它能够执行一系列相关的操作. ]]
local channel_meta = { __index = channel }

--[[ 通道的套接字的元表, 包含了两个读方法 read 和 readline, 以及在垃圾回收时候关闭套接字本身 ]]
local channel_socket_meta = {
	__index = channel_socket,
	__gc = function(cs)
		local fd = cs[1]
		cs[1] = false
		if fd then
			socket.shutdown(fd)
		end
	end
}

local socket_error = setmetatable({}, {__tostring = function() return "[Error: socket]" end })	-- alias for error object
socket_channel.error = socket_error

function socket_channel.channel(desc)
	local c = {
		__host = assert(desc.host), -- 主机地址
		__port = assert(desc.port), -- 端口号
		__backup = desc.backup, -- 备份地址, 当 __host 和 __port 连接不成功时尝试连接这个表中的主机和端口号
		__auth = desc.auth, -- 验证函数, 此函数仅有一个参数就是 c 本身
		__response = desc.response,	-- It's for session mode, 一个函数, 将从 __sock 中取得数据和会话号, 有这个函数表示是 session 模式
		__request = {},	-- request seq { response func }	-- It's for order mode
		__thread = {}, -- coroutine seq or session->coroutine map, 等待执行下一步操作的协程, 需要调用 skynet.wakeup 将它们唤醒.
		__result = {}, -- response result { coroutine -> result } 返回的结果, 任何非 nil 或者 false 值均认为是正确返回, 如果发生套接字错误将被赋值为 socket_error
		__result_data = {}, -- 跟随在结果之后的数据, 若结果为 socket_error, 数据将包含错误信息. 否则就是正常的数据内容.
		__connecting = {}, -- 当有多条线程都去连接通道时, 后续的线程将会放到此表中去等待连接结果
		__sock = false, -- 套接字本身, 当连接成功时这是一个包含套接字 id 的表
		__closed = false, -- 是否关闭, 调用 channel:close 方法将关闭此通道
		__authcoroutine = false, -- 授权协程, 仅仅在连接验证时存在
		__nodelay = desc.nodelay, -- 是否非延时
	}

	return setmetatable(c, channel_meta)
end

--[[ 关闭通道的套接字, 此函数一定不会抛出错误. ]]
local function close_channel_socket(self)
	if self.__sock then
		local so = self.__sock
		self.__sock = false
		-- never raise error
		pcall(socket.close,so[1])
	end
end

--[[ 当套接字发生错误或者关闭时, 唤醒所有在此套接字上等待的协程. 此函数将会通知所有协程发生了错误, 将相应的错误信息告知给协程.
参数: self 是 channel 本身; errmsg 是错误信息; ]]
local function wakeup_all(self, errmsg)
	if self.__response then
		for k,co in pairs(self.__thread) do
			self.__thread[k] = nil
			self.__result[co] = socket_error
			self.__result_data[co] = errmsg
			skynet.wakeup(co)
		end
	else
		for i = 1, #self.__request do
			self.__request[i] = nil
		end
		for i = 1, #self.__thread do
			local co = self.__thread[i]
			self.__thread[i] = nil
			if co then	-- ignore the close signal
				self.__result[co] = socket_error
				self.__result_data[co] = errmsg
				skynet.wakeup(co)
			end
		end
	end
end

--[[ 退出分发线程, 表示此次连接关闭. 如果关闭之前发现有连接协程在等待分发线程退出, 唤醒它. ]]
local function exit_thread(self)
	local co = coroutine.running()
	if self.__dispatch_thread == co then
		self.__dispatch_thread = nil
		local connecting = self.__connecting_thread
		if connecting then
			skynet.wakeup(connecting)
		end
	end
end

--[[ session 模式的分发主循环函数. 由 response 函数从套接字中读取数据, 依据数据的不同情况进行相应的处理.
处理之后将会唤醒等待的协程, 等待协程调用 wait_for_response 等待返回包. 当套接字关闭时将退出此分发函数.
注意: 如果对端返回多个短消息, 函数将返回一个 table 给等待协程. ]]
local function dispatch_by_session(self)
	local response = self.__response
	-- response() return session
	while self.__sock do
		--[[ ok 表示解析是否成功, result_ok 是套接字对端发送过来的状态码 ]]
		local ok , session, result_ok, result_data, padding = pcall(response, self.__sock)
		if ok and session then
			local co = self.__thread[session]
			if co then
				if padding and result_ok then
					-- If padding is true, append result_data to a table (self.__result_data[co])
					local result = self.__result_data[co] or {}
					self.__result_data[co] = result
					table.insert(result, result_data)
				else
					self.__thread[session] = nil
					self.__result[co] = result_ok
					if result_ok and self.__result_data[co] then
						table.insert(self.__result_data[co], result_data)
					else
						self.__result_data[co] = result_data
					end
					skynet.wakeup(co)
				end
			else
				self.__thread[session] = nil
				skynet.error("socket: unknown session :", session)
			end
		else
			close_channel_socket(self)
			local errormsg
			if session ~= socket_error then
				errormsg = session
			end
			wakeup_all(self, errormsg)
		end
	end
	exit_thread(self)
end

--[[ order 模式下从 channel 中返回一个读取函数和相应的等待协程. 如果返回的协程是 false 就表示需要关闭当前连接.
如果 channel 中没有更多的协程, 将等待直到新的请求协程进入.

返回: [1] function 读取套接字函数或者在关闭信号时返回 true; [2] coroutine 等待的请求协程或者在关闭信号时返回 false; ]]
local function pop_response(self)
	while true do
		local func,co = table.remove(self.__request, 1), table.remove(self.__thread, 1)
		if func then
			return func, co
		end
		self.__wait_response = coroutine.running()
		skynet.wait(self.__wait_response)
	end
end

--[[ 向 channel 中插入一个响应函数和相应的等待协程. 函数能够针对 session 模式和 order 模式分别作出不同的处理.
如果是 session 模式则要求 response 是 integer 类型的 session 值, 如果是 order 模式则要求 response 是读取函数. ]]
local function push_response(self, response, co)
	if self.__response then
		-- response is session
		self.__thread[response] = co
	else
		-- response is a function, push it to __request
		table.insert(self.__request, response)
		table.insert(self.__thread, co)
		if self.__wait_response then
			skynet.wakeup(self.__wait_response)
			self.__wait_response = nil
		end
	end
end

--[[ order 模式下的分发, 消息包将按照请求顺序依次返回, 常用于需要保证时序的请求中, 现在的场景是 Redis 驱动模块.
此函数将在读取结果失败时关闭套接字, 或者解析数据并依据不同数据返回情况作出相应的处理. 处理完之后将唤醒相应的等待协程
去完成接下来的工作. 协程等待调用 wait_for_response 函数. 当套接字关闭时将退出此分发函数.
注意: 如果对端返回多个短消息, 函数将返回一个 table 给等待协程. ]]
local function dispatch_by_order(self)
	while self.__sock do
		local func, co = pop_response(self)
		if not co then
			-- close signal
			wakeup_all(self)
			break
		end
		local ok, result_ok, result_data, padding = pcall(func, self.__sock)
		if ok then
			if padding and result_ok then
				-- if padding is true, wait for next result_data
				-- self.__result_data[co] is a table
				--[[ [ck]padding 的情况下有可能出错, 因为永远不会再把 co 入队列, pop_response 时取不到原来的 co.[/ck] ]]
				local result = self.__result_data[co] or {}
				self.__result_data[co] = result
				table.insert(result, result_data)
			else
				self.__result[co] = result_ok
				if result_ok and self.__result_data[co] then
					table.insert(self.__result_data[co], result_data)
				else
					self.__result_data[co] = result_data
				end
				skynet.wakeup(co)
			end
		else
			close_channel_socket(self)
			local errmsg
			if result_ok ~= socket_error then
				errmsg = result_ok
			end
			self.__result[co] = socket_error
			self.__result_data[co] = errmsg
			skynet.wakeup(co)
			wakeup_all(self, errmsg)
		end
	end
	exit_thread(self)
end

--[[ 依据 session 和 order 模式分别获取相应的消息分发函数. ]]
local function dispatch_function(self)
	if self.__response then
		return dispatch_by_session
	else
		return dispatch_by_order
	end
end

--[[ 当连接通道的首选地址 __host 和 __port 失败时, 进而去连接备份地址, 备份地址是一个表,
表中可能是子表标识主机 host 和端口 port, 或者仅仅是主机地址 addr. 当连接成功时将返回套接字的 id,
不成功则返回 nil. ]]
local function connect_backup(self)
	if self.__backup then
		for _, addr in ipairs(self.__backup) do
			local host, port
			if type(addr) == "table" then
				host, port = addr.host, addr.port
			else
				host = addr
				port = self.__port
			end
			skynet.error("socket: connect to backup host", host, port)
			local fd = socket.open(host, port)
			if fd then
				self.__host = host
				self.__port = port
				return fd
			end
		end
	end
end

--[[ 尝试进行一次连接, 如果通道已经是关闭着的将返回 false. 否则将发起连接, 并注册分发函数,
以及在有验证函数的情况下进行验证. 如果连接失败或者验证失败将返回 false 和错误信息, 成功则返回 true. ]]
local function connect_once(self)
	if self.__closed then
		return false
	end
	--[[ 断言当前通道还没有连接成功, 也不是正在连接 ]]
	assert(not self.__sock and not self.__authcoroutine)
	local fd,err = socket.open(self.__host, self.__port)
	if not fd then
		fd = connect_backup(self)
		if not fd then
			return false, err
		end
	end
	if self.__nodelay then
		socketdriver.nodelay(fd)
	end

	self.__sock = setmetatable( {fd} , channel_socket_meta )
	-- 开启消息分发线程
	self.__dispatch_thread = skynet.fork(dispatch_function(self), self)

	if self.__auth then
		self.__authcoroutine = coroutine.running()
		local ok , message = pcall(self.__auth, self)
		if not ok then
			close_channel_socket(self)
			if message ~= socket_error then
				self.__authcoroutine = false
				skynet.error("socket: auth failed", message)
			end
		end
		self.__authcoroutine = false
		if ok and not self.__sock then
			-- auth may change host, so connect again
			return connect_once(self)
		end
		return ok
	end

	return true
end

--[[ 尝试去连接通道, once 表示是否仅连接一次还是一直连接直到通道被关闭. 如果连接一次失败时将返回错误信息,
成功则返回 nil. 如果连接多次, 失败和成功都将记录日志, 并且其重连的时间间隔将逐步增大直到 1000 厘秒(cs). ]]
local function try_connect(self , once)
	local t = 0
	while not self.__closed do
		local ok, err = connect_once(self)
		if ok then
			if not once then
				skynet.error("socket: connect to", self.__host, self.__port)
			end
			return
		elseif once then
			return err
		else
			skynet.error("socket: connect", err)
		end
		if t > 1000 then
			skynet.error("socket: try to reconnect", self.__host, self.__port)
			skynet.sleep(t)
			t = 0
		else
			skynet.sleep(t)
		end
		t = t + 100
	end
end

--[[ 校验连接, 如果已经连接成功或者当前协程正在连接将返回 true, 如果通道已经关闭将返回 false,
而此时如果是其它协程在连接或者连接失败无 __sock 时将返回 nil. ]]
local function check_connection(self)
	if self.__sock then
		local authco = self.__authcoroutine
		if not authco then
			return true
		end
		if authco == coroutine.running() then
			-- authing
			return true
		end
	end
	if self.__closed then
		return false
	end
end

--[[ 以阻塞方式去连接通道, 当已经有第一条线程去连接时, 当前线程将会等待连接结果. 如果最终连接失败将抛出错误.
如果连接成功将返回 true, 如果通道关闭将返回 false. ]]
local function block_connect(self, once)
	local r = check_connection(self)
	if r ~= nil then
		return r
	end
	local err

	if #self.__connecting > 0 then
		-- connecting in other coroutine
		local co = coroutine.running()
		table.insert(self.__connecting, co)
		skynet.wait(co)
	else
		self.__connecting[1] = true
		err = try_connect(self, once)
		self.__connecting[1] = nil
		for i=2, #self.__connecting do
			local co = self.__connecting[i]
			self.__connecting[i] = nil
			skynet.wakeup(co)
		end
	end

	r = check_connection(self)
	if r == nil then
		error(string.format("Connect to %s:%d failed (%s)", self.__host, self.__port, err))
	else
		return r
	end
end

--[[ 将通道连接一次, 当通道是关闭着的时候, 并且存在分发线程时将只能有一条线程去连接. 而其它连接的线程将会阻塞
直到连接成功或者失败返回, 成功时返回 true, 而失败时将抛出错误, false 表示通道关闭. ]]
function channel:connect(once)
	if self.__closed then
		-- 存在分发线程, 将等待这个分发线程结束再进行连接
		if self.__dispatch_thread then
			-- closing, wait
			assert(self.__connecting_thread == nil, "already connecting")
			local co = coroutine.running()
			self.__connecting_thread = co
			skynet.wait(co)
			self.__connecting_thread = nil
		end
		self.__closed = false
	end

	return block_connect(self, once)
end

--[[ 等待对端数据返回, 当为 session 模式时 response 是整形的会话号, 当为 order 模式时 response 是读取函数.
如果发生任何套接字错误, 函数将抛出此错误. 否则函数将返回相应的返回数据. 如果对端发送的数据是多个短消息, 函数将
返回一个 table.

参数: self 为 channel 本身; response 是响应函数或者 session id, 视情况而定;
返回: [1] string or table 返回数据. ]]
local function wait_for_response(self, response)
	local co = coroutine.running()
	push_response(self, response, co)
	skynet.wait(co)

	local result = self.__result[co]
	self.__result[co] = nil
	local result_data = self.__result_data[co]
	self.__result_data[co] = nil

	if result == socket_error then
		if result_data then
			error(result_data)
		else
			error(socket_error)
		end
	else
		assert(result, result_data)
		return result_data
	end
end

local socket_write = socket.write
local socket_lwrite = socket.lwrite

--[[ 发起一个请求, 如果 response 参数不为 nil , 将等待对端返回数据. 否则将仅仅发送数据. 如果有 padding 参数,
则依次将调用套接字的低优先级发送函数, 发送所有数据.

参数: request 请求体字符串; response 不为 nil 时表示响应函数或者 session id; padding 为后续发送内容, 是一个 table;
返回: 对端响应的数据, 类型是一个 string 或者 table; ]]
function channel:request(request, response, padding)
	assert(block_connect(self, true))	-- connect once
	local fd = self.__sock[1]

	if padding then
		-- padding may be a table, to support multi part request
		-- multi part request use low priority socket write
		-- socket_lwrite returns nothing
		socket_lwrite(fd , request)
		for _,v in ipairs(padding) do
			socket_lwrite(fd, v)
		end
	else
		if not socket_write(fd , request) then
			close_channel_socket(self)
			wakeup_all(self)
			error(socket_error)
		end
	end

	if response == nil then
		-- no response
		return
	end

	return wait_for_response(self, response)
end

--[[ 等待响应, 如果为 session 模式则 response 是一个会话号, 如果是 order 模式则 response 是一个读取函数.
函数会自动重连自身 channel. 并等待响应数据. ]]
function channel:response(response)
	assert(block_connect(self))

	return wait_for_response(self, response)
end

--[[ 关闭当前 channel. 如果是 order 分发模式, 还将发送关闭信号给分发协程. 一般情况下不需要主动调用此函数.
模块会在 gc 时关闭当前连接. ]]
function channel:close()
	if not self.__closed then
		local thread = self.__dispatch_thread
		self.__closed = true
		close_channel_socket(self)
		if not self.__response and self.__dispatch_thread == thread and thread then
			-- dispatch by order, send close signal to dispatch thread
			push_response(self, true, false)	-- (true, false) is close signal
		end
	end
end

--[[ 更改当前 channel 的对端服务地址. 如果 channel 处于连接状态, 将关闭当前连接. ]]
function channel:changehost(host, port)
	self.__host = host
	if port then
		self.__port = port
	end
	if not self.__closed then
		close_channel_socket(self)
	end
end

function channel:changebackup(backup)
	self.__backup = backup
end

channel_meta.__gc = channel.close

--[[ 对套接字函数进行包装, 使之处理 channel_socket 的 fd(self[1]), 并且在处理失败时抛出套接字错误. ]]
local function wrapper_socket_function(f)
	return function(self, ...)
		local result = f(self[1], ...)
		if not result then
			error(socket_error)
		else
			return result
		end
	end
end

channel_socket.read = wrapper_socket_function(socket.read)
channel_socket.readline = wrapper_socket_function(socket.readline)

return socket_channel
