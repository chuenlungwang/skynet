local skynet = require "skynet"
local sharedata = require "sharedata.corelib"
local table = table
local cache = require "skynet.codecache"
cache.mode "OFF"	-- turn off codecache, because CMD.new may load data file

local NORET = {}
local pool = {}
local pool_count = {}
local objmap = {}

--[[ 将表 tbl 以名字 name 构建一个新的表结构. 要求对象池 pool 池中没有此表结构.
构建好之后将增加表结构的引用, 并放入到缓存池中. ]]
local function newobj(name, tbl)
	assert(pool[name] == nil)
	local cobj = sharedata.host.new(tbl)
	sharedata.host.incref(cobj)
	local v = { value = tbl , obj = cobj, watch = {} }
	objmap[cobj] = v
	pool[name] = v
	pool_count[name] = { n = 0, threshold = 16 }
end

--[[ 每隔十分钟回收已经没有被引用的表结构. ]]
local function collectobj()
	while true do
		skynet.sleep(600 * 100)	-- sleep 10 min
		collectgarbage()
		for obj, v in pairs(objmap) do
			if v == true then
				if sharedata.host.getref(obj) <= 0  then
					objmap[obj] = nil
					sharedata.host.delete(obj)
				end
			end
		end
	end
end

local CMD = {}

local env_mt = { __index = _ENV }

--[[ 以名字 name 构建表 t 的 skynet 定义的表结构. t 可以是以 @ 开头的文件或者是代码块,
或者 nil(此时表为空表), 其它类型的值将抛出错误. ]]
function CMD.new(name, t, ...)
	local dt = type(t)
	local value
	if dt == "table" then
		value = t
	elseif dt == "string" then
		value = setmetatable({}, env_mt)
		local f
		if t:sub(1,1) == "@" then
			f = assert(loadfile(t:sub(2),"bt",value))
		else
			f = assert(load(t, "=" .. name, "bt", value))
		end
		local _, ret = assert(skynet.pcall(f, ...))
		setmetatable(value, nil)
		if type(ret) == "table" then
			value = ret
		end
	elseif dt == "nil" then
		value = {}
	else
		error ("Unknown data type " .. dt)
	end
	newobj(name, value)
end

--[[ 从缓存池中将名字为 name 的表结构删除, 这将导致所有客户端的监控协程结束(监控线程用于监控表结构的更新). ]]
function CMD.delete(name)
	local v = assert(pool[name])
	pool[name] = nil
	pool_count[name] = nil
	assert(objmap[v.obj])
	objmap[v.obj] = true
	sharedata.host.decref(v.obj)
	for _,response in pairs(v.watch) do
		response(true)
	end
end

--[[ 从缓存中查询名字为 name 的表结构, 并增加引用. 随后需要客户端调用 confirm 函数减小引用. ]]
function CMD.query(name)
	local v = assert(pool[name])
	local obj = v.obj
	sharedata.host.incref(obj)
	return v.obj
end

--[[ 客户端确认一个表结构已经被正确获得, 它将减小表结构的引用.
函数无返回值, 所以需要客户端调用 skynet.send 方法. ]]
function CMD.confirm(cobj)
	if objmap[cobj] then
		sharedata.host.decref(cobj)
	end
	return NORET
end

--[[ 将名字 name 对应的表结构更新为新表 t. 更新表将会减小旧表的引用,
并在有监控客户端的情况下, 标记旧表结构为脏. 然后通知所有的监控客户端. ]]
function CMD.update(name, t, ...)
	local v = pool[name]
	local watch, oldcobj
	if v then
		watch = v.watch
		oldcobj = v.obj
		objmap[oldcobj] = true
		sharedata.host.decref(oldcobj)
		pool[name] = nil
		pool_count[name] = nil
	end
	CMD.new(name, t, ...)
	local newobj = pool[name].obj
	if watch then
		sharedata.host.markdirty(oldcobj)
		for _,response in pairs(watch) do
			response(true, newobj)
		end
	end
end

--[[ 检查监控队列中已经死去的客户端的数量有多少, 并返回这个值. ]]
local function check_watch(queue)
	local n = 0
	for k,response in pairs(queue) do
		if not response "TEST" then
			queue[k] = nil
			n = n + 1
		end
	end
	return n
end

--[[ 监控名字为 name 的表结构, 如果当前表结构已经更新了, 将立即返回. 否则将保存到监控列表中,
并在更新表结构时返回新的表结构. ]]
function CMD.monitor(name, obj)
	local v = assert(pool[name])
	if obj ~= v.obj then
		return v.obj
	end

	local n = pool_count[name].n + 1
	if n > pool_count[name].threshold then
		n = n - check_watch(v.watch)
		pool_count[name].threshold = n * 2
	end
	pool_count[name].n = n

	table.insert(v.watch, skynet.response())

	return NORET
end

skynet.start(function()
	skynet.fork(collectobj)
	skynet.dispatch("lua", function (session, source ,cmd, ...)
		local f = assert(CMD[cmd])
		local r = f(...)
		if r ~= NORET then
			skynet.ret(skynet.pack(r))
		end
	end)
end)

