local skynet = require "skynet"
local sc = require "socketchannel"
local socket = require "socket"
local cluster = require "cluster.core"

local config_name = skynet.getenv "cluster"
local node_address = {}
local node_session = {}
local command = {}

--[[ 从一个 socketchannel 的套接字中读取数据, 这个 sock 具有两个方法: read 和 readline.
函数首先从套接字中读取两个字节的头(大端形式), 再读取相应长度的数据, 并解包出来, 如果数据是由多个分包组成, 这里仅仅表示一个分包.

返回: integer [1] 本次回复对应的会话号; boolean [2] 是否处理成功; string or integer [3] 表示数据内容, 或者后续数据的大小;
      boolean [4] 表示是否还有后续数据; ]]
local function read_response(sock)
	local sz = socket.header(sock:read(2))
	local msg = sock:read(sz)
	return cluster.unpackresponse(msg)	-- session, ok, data, padding
end

--[[ 函数以 key 为键从 node_address 表中取出主机端口来生成一个 socketchannel, 并且尝试连接一次.
如果连接失败将抛出错误, 生成的 channel 将放到表 t 中去. 此函数将作为元表的 __index 域使用, 供自动连接相应的集群端口.

参数: t 就是 node_channel; key 是节点的名字, 配置在 config_name 文件中; ]]
local function open_channel(t, key)
	local host, port = string.match(node_address[key], "([^:]+):(.*)$")
	local c = sc.channel {
		host = host,
		port = tonumber(port),
		response = read_response,
		nodelay = true,
	}
	assert(c:connect(true))
	t[key] = c
	return c
end

local node_channel = setmetatable({}, { __index = open_channel })

--[[ 从 config_name 中加载集群配置, 如果之前存在连接配置且地址发生改变, 将释放原来的 socketchannel .
配置信息将被加载到 node_address 中. ]]
local function loadconfig()
	local f = assert(io.open(config_name))
	local source = f:read "*a"
	f:close()
	local tmp = {}
	assert(load(source, "@"..config_name, "t", tmp))()
	for name,address in pairs(tmp) do
		assert(type(address) == "string")
		if node_address[name] ~= address then
			-- address changed
			if rawget(node_channel, name) then
				node_channel[name] = nil	-- reset connection
			end
			node_address[name] = address
		end
	end
end

--[[ 重新加载配置并回复调用者. ]]
function command.reload()
	loadconfig()
	skynet.ret(skynet.pack(nil))
end

--[[ 开启当前 skynet 节点对外的集群端口侦听, addr 和 port 可以是主机地址和端口, 也可以仅提供 addr 为节点名称.
函数将当前服务作为本机 skynet 节点的 watchdog 提供给其它集群.

参数: source 是发起请求的服务; addr 可以是具体的 ip 地址, 也可以是配置中的节点名称; port 为端口号, 如果不提供此参数, addr 为节点名称; ]]
function command.listen(source, addr, port)
	local gate = skynet.newservice("gate")
	if port == nil then
		addr, port = string.match(node_address[addr], "([^:]+):(.*)$")
	end
	skynet.call(gate, "lua", "open", { address = addr, port = port })
	skynet.ret(skynet.pack(nil))
end

--[[ 向节点 node 中的地址为 addr 的服务发送消息 msg. 函数有可能在连接时或者请求时发生错误, 这个错误会抛出.
参数: source 是发起请求的服务; node 是请求的节点名称, 在配置中表明; addr 是服务地址; msg 是消息; sz 是消息大小; ]]
local function send_request(source, node, addr, msg, sz)
	local session = node_session[node] or 1
	-- msg is a local pointer, cluster.packrequest will free it
	local request, new_session, padding = cluster.packrequest(addr, session, msg, sz)
	node_session[node] = new_session

	-- node_channel[node] may yield or throw error
	local c = node_channel[node]

	return c:request(request, session, padding)
end

--[[ 发起请求命令, 参数与 send_request 一致. 如果调用成功将返回消息, 否则将向
发起请求的服务发送类型为 skynet.PTYPE_ERROR 的错误信息. ]]
function command.req(...)
	local ok, msg, sz = pcall(send_request, ...)
	if ok then
		if type(msg) == "table" then
			skynet.ret(cluster.concat(msg))
		else
			skynet.ret(msg)
		end
	else
		skynet.error(msg)
		skynet.response()(false)
	end
end

local proxy = {}

--[[ 获取远程服务的代理服务地址, 如果系统中不存在此代理服务将会创建它. 如果存在则复用.
参数: source 是请求发起服务; node 是集群中远程节点名称; name 是远程服务地址或名字; ]]
function command.proxy(source, node, name)
	local fullname = node .. "." .. name
	if proxy[fullname] == nil then
		proxy[fullname] = skynet.newservice("clusterproxy", node, name)
	end
	skynet.ret(skynet.pack(proxy[fullname]))
end

local register_name = {}

--[[ 给服务注册名字, 这些注册的名字供外部集群查询, 不属于 skynet 内置的名字系统.
函数要求当前的名字没有被注册, 并且如果不提供 addr 则将请求服务注册为名字 name. ]]
function command.register(source, name, addr)
	assert(register_name[name] == nil)
	addr = addr or source
	local old_name = register_name[addr]
	if old_name then
		register_name[old_name] = nil
	end
	register_name[addr] = name
	register_name[name] = addr
	skynet.ret(nil)
	skynet.error(string.format("Register [%s] :%08x", name, addr))
end

local large_request = {}

--[[ 类似于 watchdog 的接口, 提供给 gate 服务使用. 分别会在建立连接、接收数据、断开连接时调用.
接收到消息, 则转发给相应的服务并将结果回复给对应的远程请求方. ]]
function command.socket(source, subcmd, fd, msg)
	if subcmd == "data" then
		local sz
		local addr, session, msg, padding = cluster.unpackrequest(msg)
		--[[ 拼接大的请求 ]]
		if padding then
			local req = large_request[session] or { addr = addr }
			large_request[session] = req
			table.insert(req, msg)
			return
		else
			local req = large_request[session]
			if req then
				large_request[session] = nil
				table.insert(req, msg)
				msg,sz = cluster.concat(req)
				addr = req.addr
			end
			if not msg then
				local response = cluster.packresponse(session, false, "Invalid large req")
				socket.write(fd, response)
				return
			end
		end
		--[[ addr 为 0 时表示查询名字, 否则转发到相应的服务去 ]]
		local ok, response
		if addr == 0 then
			local name = skynet.unpack(msg, sz)
			local addr = register_name[name]
			if addr then
				ok = true
				msg, sz = skynet.pack(addr)
			else
				ok = false
				msg = "name not found"
			end
		else
			ok , msg, sz = pcall(skynet.rawcall, addr, "lua", msg, sz)
		end
		if ok then
			response = cluster.packresponse(session, true, msg, sz)
			if type(response) == "table" then
				for _, v in ipairs(response) do
					socket.lwrite(fd, v)
				end
			else
				socket.write(fd, response)
			end
		else
			response = cluster.packresponse(session, false, msg)
			socket.write(fd, response)
		end
	elseif subcmd == "open" then
		skynet.error(string.format("socket accept from %s", msg))
		skynet.call(source, "lua", "accept", fd)
	else
		large_request = {}
		skynet.error(string.format("socket %s %d : %s", subcmd, fd, msg))
	end
end

skynet.start(function()
	loadconfig()
	skynet.dispatch("lua", function(session , source, cmd, ...)
		local f = assert(command[cmd])
		f(source, ...)
	end)
end)
