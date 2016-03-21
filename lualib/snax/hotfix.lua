local si = require "snax.interface"

--[[ 查找函数 f 的 _ENV 的唯一标识符, 一个轻量用户数据. ]]
local function envid(f)
	local i = 1
	while true do
		local name, value = debug.getupvalue(f, i)
		if name == nil then
			return
		end
		if name == "_ENV" then
			return debug.upvalueid(f, i)
		end
		i = i + 1
	end
end

--[[ 收集函数 f 的上值到 uv 表中, 要求所有重名的上值必须指向同一个值, env 是 _ENV 上值. ]]
local function collect_uv(f , uv, env)
	local i = 1
	while true do
		local name, value = debug.getupvalue(f, i)
		if name == nil then
			break
		end
		local id = debug.upvalueid(f, i)

		--[[ 所有这些函数必须共享相同名字的上值, 即在函数外部定义的变量, 必须是不重名的. ]]
		if uv[name] then
			assert(uv[name].id == id, string.format("ambiguity local value %s", name))
		else
			uv[name] = { func = f, index = i, id = id }

			-- 如果上值是一个函数, 也收集此函数的所有上值
			if type(value) == "function" then
				if envid(value) == env then
					collect_uv(value, uv, env)
				end
			end
		end

		i = i + 1
	end
end

local function collect_all_uv(funcs)
	local global = {}
	for _, v in pairs(funcs) do
		if v[4] then
			collect_uv(v[4], global, envid(v[4]))
		end
	end
	if not global["_ENV"] then
		global["_ENV"] = {func = collect_uv, index = 1}
	end
	return global
end

--[[ 返回一个加载函数, 这个函数并不从文件名中加载代码,
而是加载 source 中的代码, 此函数提供给 interface 模块使用. ]]
local function loader(source)
	return function (filename, ...)
		return load(source, "=patch", ...)
	end
end

--[[ 从表 funcs 中查找相应的方法表, 表的形式: desc = {id, group, name, f} ]]
local function find_func(funcs, group , name)
	for _, desc in pairs(funcs) do
		local _, g, n = table.unpack(desc)
		if group == g and name == n then
			return desc
		end
	end
end

local dummy_env = {}

local function patch_func(funcs, global, group, name, f)
	local desc = assert(find_func(funcs, group, name) , string.format("Patch mismatch %s.%s", group, name))
	-- 让新函数的上值全部引用同名的旧函数的上值, 达到一个热更新的目的
	local i = 1
	while true do
		local name, value = debug.getupvalue(f, i)
		if name == nil then
			break
		elseif value == nil or value == dummy_env then
			local old_uv = global[name]
			if old_uv then
				debug.upvaluejoin(f, i, old_uv.func, old_uv.index)
			end
		end
		i = i + 1
	end
	desc[4] = f
end

--[[ 将补丁函数注入到原来的函数表中去, 并调用 hotfix 函数. ]]
local function inject(funcs, source, ...)
	local patch = si("patch", dummy_env, loader(source))
	local global = collect_all_uv(funcs)

	for _, v in pairs(patch) do
		local _, group, name, f = table.unpack(v)
		if f then
			patch_func(funcs, global, group, name, f)
		end
	end

	local hf = find_func(patch, "system", "hotfix")
	if hf and hf[4] then
		return hf[4](...)
	end
end

--[[ 将补丁函数从代码字符串 source 中加载, 并注入到函数表 funcs 中去. ]]
return function (funcs, source, ...)
	return pcall(inject, funcs, source, ...)
end
