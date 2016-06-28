local skynet = require "skynet"
local sd = require "sharedata.corelib"

local service

--[[ 在服务启动前先启动唯一服务 sharedatad ]]
skynet.init(function()
	service = skynet.uniqueservice "sharedatad"
end)

local sharedata = {}

--[[ 监控协程, 每次以当前的表结构去监控服务端的表结构, 服务端会在更新了表结构或者删除了表结构时返回.
删除了表结构将返回 nil. 从而完成表结构的更新或者结束监控. ]]
local function monitor(name, obj, cobj)
	local newobj = cobj
	while true do
		newobj = skynet.call(service, "lua", "monitor", name, newobj)
		if newobj == nil then
			break
		end
		sd.update(obj, newobj)
	end
end

--[[ 从服务端查询名字 name 的表结构, 并不断监控其更新. ]]
function sharedata.query(name)
	local obj = skynet.call(service, "lua", "query", name)
	local r = sd.box(obj)
	skynet.send(service, "lua", "confirm" , obj)
	skynet.fork(monitor,name, r, obj)
	return r
end

function sharedata.new(name, v, ...)
	skynet.call(service, "lua", "new", name, v, ...)
end

function sharedata.update(name, v, ...)
	skynet.call(service, "lua", "update", name, v, ...)
end

function sharedata.delete(name)
	skynet.call(service, "lua", "delete", name)
end

return sharedata
