local skynet = require "skynet"
local core = require "skynet.core"
require "skynet.manager"	-- import manager apis
local string = string

local services = {}
local command = {}
local instance = {} -- for confirm (function command.LAUNCH / command.ERROR / command.LAUNCHOK)

--[[ 将冒号开头的16进制服务地址的字符串转为整数 ]]
local function handle_to_address(handle)
	return tonumber("0x" .. string.sub(handle , 2))
end

local NORET = {}

--[[ 列举出所有由 launcher 服务启动且还存活的服务,
以表的形式返回, 其中键是冒号开头的 16 进制服务地址字符串,值是启动命令. ]]
function command.LIST()
	local list = {}
	for k,v in pairs(services) do
		list[skynet.address(k)] = v
	end
	return list
end

--[[ 查看所有由 launcher 服务启动且还存活的服务的状态,
以表的形式返回. 其中键是冒号开头的 16 进制服务地址字符串,值是状态. ]]
function command.STAT()
	local list = {}
	for k,v in pairs(services) do
		local ok, stat = pcall(skynet.call,k,"debug","STAT")
		if not ok then
			stat = string.format("ERROR (%s)",v)
		end
		list[skynet.address(k)] = stat
	end
	return list
end

--[[ 杀掉地址为 handle 的服务, 并返回一个表. 表的唯一键是其冒号
开头的 16 进制服务地址字符串, 值是启动命令.

参数: int _ 为请求服务地址; int handle 是待杀死的服务地址; ]]
function command.KILL(_, handle)
	handle = handle_to_address(handle)
	skynet.kill(handle)
	local ret = { [skynet.address(handle)] = tostring(services[handle]) }
	services[handle] = nil
	return ret
end

--[[ 查看所有由 launcher 服务启动且还存活的服务的内存使用情况, 并返回一个表.
表的唯一键是其冒号开头的 16 进制服务地址字符串, 值是字符串形式的使用情况. ]]
function command.MEM()
	local list = {}
	for k,v in pairs(services) do
		local ok, kb, bytes = pcall(skynet.call,k,"debug","MEM")
		if not ok then
			list[skynet.address(k)] = string.format("ERROR (%s)",v)
		else
			list[skynet.address(k)] = string.format("%.2f Kb (%s)",kb,v)
		end
	end
	return list
end

--[[ 对所有由 launcher 服务启动且还存活的服务做一个垃圾回收处理.
函数最终将返回所有服务的内存使用情况. ]]
function command.GC()
	for k,v in pairs(services) do
		skynet.send(k,"debug","GC")
	end
	return command.MEM()
end

--[[ 从 launcher 服务的内部表中移除服务 handle, 如果是需要杀死且启动还没有完毕
将同时返回给发起启动服务的服务一个错误, 否则将返回 nil 给它作为最终的服务地址. ]]
function command.REMOVE(_, handle, kill)
	services[handle] = nil
	local response = instance[handle]
	if response then
		-- instance is dead
		response(not kill)	-- return nil to caller of newservice, when kill == false
		instance[handle] = nil
	end

	-- don't return (skynet.ret) because the handle may exit
	return NORET
end

--[[ 启动一个服务, 如果启动成功将不立即返回, 而是等待被启动的服务发送 LAUNCHOK 命令
过来才返回给发起启动的服务. 如果启动失败将立即返回错误给发起启动的服务. ]]
local function launch_service(service, ...)
	local param = table.concat({...}, " ")
	local inst = skynet.launch(service, param)
	local response = skynet.response()
	if inst then
		services[inst] = service .. " " .. param
		instance[inst] = response
	else
		response(false)
		return
	end
	return inst
end

--[[ 启动一个服务 ]]
function command.LAUNCH(_, service, ...)
	launch_service(service, ...)
	return NORET
end

--[[ 启动一个服务, 并给这个服务打印所有消息的日志. ]]
function command.LOGLAUNCH(_, service, ...)
	local inst = launch_service(service, ...)
	if inst then
		core.command("LOGON", skynet.address(inst))
	end
	return NORET
end

--[[ 由被启动的服务调用, 通知启动服务的调用当前启动失败了. 这将返回一个 error 类型的消息给它.
这个函数会在 service_snlua.c 的 _init 函数和 skynet.lua 的 skynet.init_service 函数中调用. ]]
function command.ERROR(address)
	-- see serivce-src/service_snlua.c
	-- init failed
	local response = instance[address]
	if response then
		response(false)
		instance[address] = nil
	end
	services[address] = nil
	return NORET
end

--[[ 由被启动的服务调用, 通知启动服务的调用当前启动成功了. 这将返回被启动服务的地址给启动服务. 地址是 int 类型. ]]
function command.LAUNCHOK(address)
	-- init notice
	local response = instance[address]
	if response then
		response(true, address)
		instance[address] = nil
	end

	return NORET
end

-- for historical reasons, launcher support text command (for C service)

--[[ 注册 text 类型的协议, 目前仅提供给 service_snlua.c 使用, 用于通知启动服务, 被启动服务是否已经正确启动或者启动失败. ]]
skynet.register_protocol {
	name = "text",
	id = skynet.PTYPE_TEXT,
	unpack = skynet.tostring,
	dispatch = function(session, address , cmd)
		if cmd == "" then
			command.LAUNCHOK(address)
		elseif cmd == "ERROR" then
			command.ERROR(address)
		else
			error ("Invalid text command " .. cmd)
		end
	end,
}

--[[ 提供给其它的 Lua 服务使用的协议, 其中一些命令用于管理和审查由 launcher 启动的服务.
LAUNCHOK 和 ERROR 命令则用于上报是否启动成功. ]]
skynet.dispatch("lua", function(session, address, cmd , ...)
	cmd = string.upper(cmd)
	local f = command[cmd]
	if f then
		local ret = f(address, ...)
		if ret ~= NORET then
			skynet.ret(skynet.pack(ret))
		end
	else
		skynet.ret(skynet.pack {"Unknown command"} )
	end
end)

skynet.start(function() end)
