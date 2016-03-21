local skynet = require "skynet"
local snax_interface = require "snax.interface"

local snax = {}
local typeclass = {}

local interface_g = skynet.getenv("snax_interface_g")
local G = interface_g and require (interface_g) or { require = function() end }
interface_g = nil

skynet.register_protocol {
	name = "snax",
	id = skynet.PTYPE_SNAX,
	pack = skynet.pack,
	unpack = skynet.unpack,
}

--[[ 从 snax 路径通赔符的目录中加载名字为 name 的 Lua 文件, 并将注册函数置入表中返回. 已经加载过的文件,
则其注册函数将会缓存. ]]
function snax.interface(name)
	if typeclass[name] then
		return typeclass[name]
	end

	local si = snax_interface(name, G)

	local ret = {
		name = name,
		accept = {},
		response = {},
		system = {},
	}

	for _,v in ipairs(si) do
		local id, group, name, f = table.unpack(v)
		ret[group][name] = id
	end

	typeclass[name] = ret
	return ret
end

local meta = { __tostring = function(v) return string.format("[%s:%x]", v.type, v.handle) end}

local skynet_send = skynet.send
local skynet_call = skynet.call

--[[ 生成一个表, 这个表的 __index 元函数, 此函数将会返回 snax.interface 中 accept 下的函数. 调用返回的函数将会远程调用服务中的相应函数.
参数: table type 是调用 snax.interface(name) 返回的表; int handle 是以这个名字启动的服务;
返回: 一个函数, 调用函数将相对应的远程调用服务 handle 中的函数. ]]
local function gen_post(type, handle)
	return setmetatable({} , {
		__index = function( t, k )
			local id = type.accept[k]
			if not id then
				error(string.format("post %s:%s no exist", type.name, k))
			end
			return function(...)
				skynet_send(handle, "snax", id, ...)
			end
		end })
end

--[[ 生成一个表, 这个表的 __index 元函数, 此函数将会返回 snax.interface 中 response 下的函数. 调用返回的函数将会远程调用服务中的相应函数.
参数: table type 是调用 snax.interface(name) 返回的表; int handle 是以这个名字启动的服务;
返回: 一个函数, 调用函数将相对应的远程调用服务 handle 中的函数. ]]
local function gen_req(type, handle)
	return setmetatable({} , {
		__index = function( t, k )
			local id = type.response[k]
			if not id then
				error(string.format("request %s:%s no exist", type.name, k))
			end
			return function(...)
				return skynet_call(handle, "snax", id, ...)
			end
		end })
end

--[[ 返回一个表, 这个表包含了 post 和 req 字表, 其中分别包含了服务 handle 中的 accept 和 response 全局表中的方法.
调用其中的函数将会远程调用服务中的相应函数.

参数: int handle 是服务句柄; string name 是 snax 服务的名字; table type 是调用 snax.interface(name) 返回的表; ]]
local function wrapper(handle, name, type)
	return setmetatable ({
		post = gen_post(type, handle),
		req = gen_req(type, handle),
		type = name,
		handle = handle,
		}, meta)
end

local handle_cache = setmetatable( {} , { __mode = "kv" } )

--[[ 生成名字为 name 的 snax 服务, 并返回其句柄 handle. 如果服务有 init 方法, 将会调用它. ]]
function snax.rawnewservice(name, ...)
	local t = snax.interface(name)
	local handle = skynet.newservice("snaxd", name)
	assert(handle_cache[handle] == nil)
	if t.system.init then
		skynet.call(handle, "snax", t.system.init, ...)
	end
	return handle
end

--[[ snax 服务的句柄是 handle 而其文件名是 type, 调用此函数将返回其接口表,
其中的 post 和 req 分别对应服务的 accept 和 response 表中的函数. 这些接口会缓存在表中. ]]
function snax.bind(handle, type)
	local ret = handle_cache[handle]
	if ret then
		assert(ret.type == type)
		return ret
	end
	local t = snax.interface(type)
	ret = wrapper(handle, type, t)
	handle_cache[handle] = ret
	return ret
end

--[[ 启动一个名字为 name 的 snax 服务, 并返回其接口. ]]
function snax.newservice(name, ...)
	local handle = snax.rawnewservice(name, ...)
	return snax.bind(handle, name)
end

local function service_name(global, name, ...)
	if global == true then
		return name
	else
		return global
	end
end

function snax.uniqueservice(name, ...)
	local handle = assert(skynet.call(".service", "lua", "LAUNCH", "snaxd", name, ...))
	return snax.bind(handle, name)
end

function snax.globalservice(name, ...)
	local handle = assert(skynet.call(".service", "lua", "GLAUNCH", "snaxd", name, ...))
	return snax.bind(handle, name)
end

function snax.queryservice(name)
	local handle = assert(skynet.call(".service", "lua", "QUERY", "snaxd", name))
	return snax.bind(handle, name)
end

function snax.queryglobal(name)
	local handle = assert(skynet.call(".service", "lua", "GQUERY", "snaxd", name))
	return snax.bind(handle, name)
end

function snax.kill(obj, ...)
	local t = snax.interface(obj.type)
	skynet_call(obj.handle, "snax", t.system.exit, ...)
end

function snax.self()
	return snax.bind(skynet.self(), SERVICE_NAME)
end

function snax.exit(...)
	snax.kill(snax.self(), ...)
end

local function test_result(ok, ...)
	if ok then
		return ...
	else
		error(...)
	end
end

function snax.hotfix(obj, source, ...)
	local t = snax.interface(obj.type)
	return test_result(skynet_call(obj.handle, "snax", t.system.hotfix, source, ...))
end

function snax.printf(fmt, ...)
	skynet.error(string.format(fmt, ...))
end

return snax
