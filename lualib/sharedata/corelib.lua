local core = require "sharedata.core"
local type = type
local next = next
local rawget = rawget

local conf = {}

conf.host = {
	new = core.new,
	delete = core.delete,
	getref = core.getref,
	markdirty = core.markdirty,
	incref = core.incref,
	decref = core.decref,
}

local meta = {}

local isdirty = core.isdirty
local index = core.index
local needupdate = core.needupdate
local len = core.len

--[[ 查找表的根节点, self 是从 skynet 表结构中查找出来的子表 ]]
local function findroot(self)
	while self.__parent do
		self = self.__parent
	end
	return self
end

--[[ 执行从上而下的更新表结构为新的, 函数只会更新表中原来已经有的子表, 而不会更新新增的子表.
如果旧的子表在新表结构中不存在将被从缓存中删除. ]]
local function update(root, cobj, gcobj)
	root.__obj = cobj
	root.__gcobj = gcobj
	local children = root.__cache
	if children then
		for k,v in pairs(children) do
			local pointer = index(cobj, k)
			if type(pointer) == "userdata" then
				update(v, pointer, gcobj)
			else
				children[k] = nil
			end
		end
	end
end

--[[ 按照从根表的键到当前表的键用点号(.)连接起来生成一个全路径键. ]]
local function genkey(self)
	local key = tostring(self.__key)
	while self.__parent do
		self = self.__parent
		key = self.__key .. "." .. key
	end
	return key
end

--[[ 获取当前表的 skynet 表结构, 如果表结构更新了, 将返回最新的表结构.
如果在新的表结构中删除了, 那么将抛出错误. ]]
local function getcobj(self)
	local obj = self.__obj
	if isdirty(obj) then
		local newobj, newtbl = needupdate(self.__gcobj)
		if newobj then
			local newgcobj = newtbl.__gcobj
			local root = findroot(self)
			update(root, newobj, newgcobj)
			--[[ 相等便表示在更新之后的指针与原来的一样, 其实就是没有更新而是被删除了 ]]
			if obj == self.__obj then
				error ("The key [" .. genkey(self) .. "] doesn't exist after update")
			end
			obj = self.__obj
		end
	end
	return obj
end

--[[ 按照键获得最新的值, 如果表结构已经更新了, 那么将以最新的表结构进行索引. 如果键对应的子表是原来没有缓存的,
将会添加到缓存中去, 如果是已经存在将会返回缓存中的那个子表(这个子表也随表结构一起更新了). 如果键对应的值是
布尔、字符串、数字将直接返回. 如果当前表已经被从新的表结构中删除了, 将抛出错误. ]]
function meta:__index(key)
	local obj = getcobj(self)
	local v = index(obj, key)
	if type(v) == "userdata" then
		local children = self.__cache
		if children == nil then
			children = {}
			self.__cache = children
		end
		local r = children[key]
		if r then
			return r
		end
		r = setmetatable({
			__obj = v,
			__gcobj = self.__gcobj,
			__parent = self,
			__key = key,
		}, meta)
		children[key] = r
		return r
	else
		return v
	end
end

--[[ 获取当前表的最新的长度. 如果表结构更新了, 那么长度将反应这种更新.
如果当前表已经被从新的表结构中删除了, 将抛出错误. ]]
function meta:__len()
	return len(getcobj(self))
end

--[[ 当前表的 __pairs 函数. 返回 next 迭代器函数、表本身以及第一个遍历键 nil.
此函数将用于通用 for 语句. ]]
function meta:__pairs()
	return conf.next, self, nil
end

--[[ 当前表的 next 函数, 与 Lua 标准定义的 next 函数结构一样.
第一个参数是要遍历的表, 第二个参数是表中的某个键. next 返回该键的下一个键及其关联的值.
如果表结构更新了, 那么此函数将反应那种更新. ]]
function conf.next(obj, key)
	local cobj = getcobj(obj)
	local nextkey = core.nextkey(cobj, key)
	if nextkey then
		return nextkey, obj[nextkey]
	end
end

--[[ 将当前表结构包装成在 Lua 中使用的表, 它拥有表需要的元方法, 并可以像表一样使用.
它的子表会随着表结构的更新而一起更新. 所以在任何时候得到的值都是最新的. ]]
function conf.box(obj)
	local gcobj = core.box(obj)
	return setmetatable({
		__parent = false,
		__obj = obj,
		__gcobj = gcobj,
		__key = "",
	} , meta)
end

--[[ 当服务端更新了表结构之后, 将新的表结构更新到此表中去, 并在随后执行各种操作时得到最新的值. ]]
function conf.update(self, pointer)
	local cobj = self.__obj
	assert(isdirty(cobj), "Only dirty object can be update")
	core.update(self.__gcobj, pointer, { __gcobj = core.box(pointer) })
end

return conf