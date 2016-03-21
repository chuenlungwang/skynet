local skynet = require "skynet"
local c = require "skynet.core"
local snax_interface = require "snax.interface"
local profile = require "profile"
local snax = require "snax"

local snax_name = tostring(...)
local func, pattern = snax_interface(snax_name, _ENV)

-- 名字为 snax_name 的子目录也被包含到查找目录中去, snax_path = ./service/snax_name/
local snax_path = pattern:sub(1,pattern:find("?", 1, true)-1) .. snax_name ..  "/"
package.path = snax_path .. "?.lua;" .. package.path

SERVICE_NAME = snax_name
SERVICE_PATH = snax_path

local profile_table = {}

local function update_stat(name, ti)
	local t = profile_table[name]
	if t == nil then
		t = { count = 0,  time = 0 }
		profile_table[name] = t
	end
	t.count = t.count + 1
	t.time = t.time + ti
end

local traceback = debug.traceback

local function return_f(f, ...)
	return skynet.ret(skynet.pack(f(...)))
end

--[[ 分别调用 accept 和 response 表中的函数, 并记录调用次数和运行时间统计. 发生错误将抛出错误.
其中 accept 中的函数是没有返回值的, 因而需要用 send 调用, 而 response 应当使用 call 调用. ]]
local function timing( method, ... )
	local err, msg
	profile.start()
	if method[2] == "accept" then
		-- no return
		err,msg = xpcall(method[4], traceback, ...)
	else
		err,msg = xpcall(return_f, traceback, method[4], ...)
	end
	local ti = profile.stop()
	update_stat(method[3], ti)
	assert(err,msg)
end

skynet.start(function()
	local init = false
	local function dispatcher( session , source , id, ...)
		local method = func[id]

		if method[2] == "system" then
			local command = method[3]
			if command == "hotfix" then
				local hotfix = require "snax.hotfix"
				skynet.ret(skynet.pack(hotfix(func, ...)))
			elseif command == "init" then
				assert(not init, "Already init")
				local initfunc = method[4] or function() end
				initfunc(...)
				skynet.ret()
				--[[ 将返回运行时间统计表的函数注册为 debug 模块的 INFO 函数 ]]
				skynet.info_func(function()
					return profile_table
				end)
				init = true
			else
				assert(init, "Never init")
				assert(command == "exit")
				local exitfunc = method[4] or function() end
				exitfunc(...)
				skynet.ret()
				init = false
				skynet.exit()
			end
		else
			assert(init, "Init first")
			timing(method, ...)
		end
	end
	skynet.dispatch("snax", dispatcher)

	-- set lua dispatcher
	function snax.enablecluster()
		skynet.dispatch("lua", dispatcher)
	end
end)
