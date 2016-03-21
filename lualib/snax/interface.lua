local skynet = require "skynet"

--[[ 返回一个函数, 这个函数能够从 snax 通配路径下加下名字 name 的 Lua 文件, 并将其中
全局表 accept, response 中的函数, init, exit 函数按照既定的顺序返回. 这个既定顺序就是
{id, group, name, func} 其中 id 是整数并且每个类型都不一样, group 的值为 "accept"、"response"、"system" 中的一个,
name 则为这些表中的函数名, init 和 exit 在 "system" 的表中. func 为函数对象. ]]
return function (name , G, loader)
	loader = loader or loadfile
	local mainfunc

	--[[ 返回一个表, 这个表的 __newindex 元方法会将名字 name 和函数 func 一起插入到传入的 id 表中去.
	group 要求必须是 "accept" 和 "response" ]]
	local function func_id(id, group)
		local tmp = {}
		local function count( _, name, func)
			if type(name) ~= "string" then
				error (string.format("%s method only support string", group))
			end
			if type(func) ~= "function" then
				error (string.format("%s.%s must be function"), group, name)
			end
			if tmp[name] then
				error (string.format("%s.%s duplicate definition", group, name))
			end
			tmp[name] = true
			table.insert(id, { #id + 1, group, name, func} )
		end
		return setmetatable({}, { __newindex = count })
	end

	do
		assert(getmetatable(G) == nil)
		assert(G.init == nil)
		assert(G.exit == nil)

		assert(G.accept == nil)
		assert(G.response == nil)
	end

	local temp_global = {}
	local env = setmetatable({} , { __index = temp_global })
	local func = {}

	--[[ hotfix 仅仅在加载补丁时, 在补丁中调用 ]]
	local system = { "init", "exit", "hotfix" }

	do
		for k, v in ipairs(system) do
			system[v] = k
			func[k] = { k , "system", v }
		end
	end

	env.accept = func_id(func, "accept")
	env.response = func_id(func, "response")

	--[[ 将 "init", "exit", "hotfix" 函数放入到 func 表中去, 而将其它除 "accept" 和 "response" 的全局变量
	放到 temp_global 中去. ]]
	local function init_system(t, name, f)
		local index = system[name]
		if index then
			if type(f) ~= "function" then
				error (string.format("%s must be a function", name))
			end
			func[index][4] = f
		else
			temp_global[name] = f
		end
	end

	local pattern

	--[[ 从 snax 路径下查找相应的文件, 并调用 chunck 来注册这些函数到表中. ]]
	do
		local path = assert(skynet.getenv "snax" , "please set snax in config file")

		local errlist = {}

		for pat in string.gmatch(path,"[^;]+") do
			local filename = string.gsub(pat, "?", name)
			local f , err = loader(filename, "bt", G)
			if f then
				pattern = pat
				mainfunc = f
				break
			else
				table.insert(errlist, err)
			end
		end

		if mainfunc == nil then
			error(table.concat(errlist, "\n"))
		end
	end

	setmetatable(G,	{ __index = env , __newindex = init_system })
	local ok, err = pcall(mainfunc)
	setmetatable(G, nil)
	assert(ok,err)

	for k,v in pairs(temp_global) do
		G[k] = v
	end

	return func, pattern
end
