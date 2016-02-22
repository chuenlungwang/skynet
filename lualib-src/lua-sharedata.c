#include <lua.h>
#include <lauxlib.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "atomic.h"

#define KEYTYPE_INTEGER 0
#define KEYTYPE_STRING 1

#define VALUETYPE_NIL 0
#define VALUETYPE_REAL 1
#define VALUETYPE_STRING 2
#define VALUETYPE_BOOLEAN 3
#define VALUETYPE_TABLE 4
#define VALUETYPE_INTEGER 5

struct table;

/* 保存在表中的值, 值的类型可以有浮点数、整数、表、字符串和布尔值. nil 类型的值是不保存的. */
union value {
	lua_Number n;         /* 浮点数类型值 */
	lua_Integer d;        /* 整数类型值 */
	struct table * tbl;   /* 表类型值, 此表为 skynet 定义的结构 */
	int string;           /* 字符串类型值, 为在 Lua 虚拟机栈上的位置, 其中保存真正的字符串 */
	int boolean;          /* 布尔类型值 */
};

/* skynet 定义的表结构中的节点, 此节点用于保存键值对. 按照键查找值时, 先取得键的哈希值从而得到在哈希数组中
 * 的位置, 检查节点链上的节点的键的哈希值、键的类型和键的内容是否完全等于用于查找的键, 如果相同则为找到. */
struct node {
	union value v;           /* 键值对中的值 */
	int key;	             // integer key or index of string table
	                         /* 若为整数则是整数值, 若为字符串则为为在 Lua 虚拟机栈上的位置 */
	int next;	             // next slot index
	                         /* 当有多个节点定位在哈希数组的同一位置时, 连接起来的下一个节点位置, 默认为 -1 */
	uint32_t keyhash;        /* 键的哈希值 */
	uint8_t keytype;	     // key type must be integer or string
	uint8_t valuetype;	     // value type can be number/string/boolean/table
	uint8_t nocolliding;	 // 0 means colliding slot 是否有多个节点定位在哈希数组的同一位置
};

/* skynet 定义的表的状态, 为表中自带的虚拟机栈的位置 1 上的完全用户数据. */
struct state {
	int dirty;              /* 是否为脏数据 */
	int ref;                /* 引用数量 */
	struct table * root;    /* skynet 中定义的表, 为最外边的表而不是子表 */
};

/* skynet 定义的表结构, 用于完全表示 Lua 中的表, 只是约束了键必须是整数或字符串而值必须是数字、字符串、布尔值或者表.
 * 此结构既可以表示最外边的表, 也可以表示表中的子表. 表由两部分构成, 一部分是序列, 即所有键为连续的从零开始的整数键.
 * 另外一部分是哈希键值对. 定义这样的结构能够在多个服务之间共享. 此结构主要用于不经常更新的数据, 每次更新都是全部更新. */
struct table {
	int sizearray;          /* 序列的大小 */
	int sizehash;           /* 哈希键值对的大小 */
	uint8_t *arraytype;     /* 序列中的所有值的类型, 这是一个长度为 sizearray 的数组 */
	union value * array;    /* 序列中的所有值, 这是一个长度为 sizearray 的数组 */
	struct node * hash;     /* 哈希键值对的所有节点数组,  */
	lua_State * L;          /* 保存字符串和 root 表状态的虚拟机栈, root 表和子表共享同一个虚拟机栈 */
};

/* 在使用 Lua 表构建 skynet 表结构时定义的上下文. 其中 Lua 虚拟机栈 L 用于保存所有表中的字符串. */
struct context {
	lua_State * L;         /* 保存字符串和 root 表状态的虚拟机栈, root 表和子表共享同一个虚拟机栈 */
	struct table * tbl;    /* 当前正在构建的表 */
	int string_index;      /* 最新的字符串的索引, 随着每次新增字符串而增加 */
};

/* 使用共享表结构的客户端包装对象, 当表结构更新了之后, 将会更新 update 表结构为新的表结构, 从而让客户端能够生成新的包装结构 */
struct ctrl {
	struct table * root;      /* 生成包装结构时的表结构, 一经设置将不会再改变 */
	struct table * update;    /* 最初时时 NULL, 当更新表结构时会将此设置为新的表结构用于生成新的包装结构 */
};

/* 计数虚拟机栈位置 1 处的表中哈希键的数目, 所有的键均不在序列中. 表中的键必须是整数、字符串或者表, 否则将抛出错误.
 * 参数: L 为包含 Lua 表在栈位置 1 的虚拟机栈; sizearray 为此 Lua 表中的序列的长度;
 * 返回: 哈希键的个数 */
static int
countsize(lua_State *L, int sizearray) {
	int n = 0;
	lua_pushnil(L);
	while (lua_next(L, 1) != 0) {
		int type = lua_type(L, -2);
		++n;
		if (type == LUA_TNUMBER) {
			if (!lua_isinteger(L, -2)) {
				luaL_error(L, "Invalid key %f", lua_tonumber(L, -2));
			}
			lua_Integer nkey = lua_tointeger(L, -2);
			if (nkey > 0 && nkey <= sizearray) {
				--n;
			}
		/* [ck]键可以是 LUA_TTABLE 吗? 测试过键的类型不能为 LUA_TTABLE[/ck] */
		} else if (type != LUA_TSTRING && type != LUA_TTABLE) {
			luaL_error(L, "Invalid key type %s", lua_typename(L, type));
		}
		lua_pop(L, 1);
	}

	return n;
}

/* 计算字符串 str 的哈希值, 其中 l 是字符串的长度. */
static uint32_t
calchash(const char * str, size_t l) {
	uint32_t h = (uint32_t)l;
	size_t l1;
	size_t step = (l >> 5) + 1;
	for (l1 = l; l1 >= step; l1 -= step) {
		h = h ^ ((h<<5) + (h>>2) + (uint8_t)(str[l1 - 1]));
	}
	return h;
}

/* 获取字符串在 Lua 虚拟机栈位置 1 处表中的索引, 这个索引并不是序列的索引, 而是以字符串作为键对应的整数值.
 * 整数值的最大值保存在 ctx 的 string_index 字段中, 如果表中尚未包含此字符串, 将添加此字符串并返回自增后
 * 的索引值. 函数会保存 Lua 虚拟机栈的平衡.
 *
 * 参数: ctx 是构建 skynet 表结构时定义的上下文; str 是需要保存在表中的字符串; sz 是字符串的长度;
 * 返回: 字符串的数字表示. */
static int
stringindex(struct context *ctx, const char * str, size_t sz) {
	lua_State *L = ctx->L;
	lua_pushlstring(L, str, sz);
	lua_pushvalue(L, -1);
	lua_rawget(L, 1);
	int index;
	// stringmap(1) str index
	if (lua_isnil(L, -1)) {
		index = ++ctx->string_index;
		lua_pop(L, 1);
		lua_pushinteger(L, index);
		lua_rawset(L, 1);
	} else {
		index = lua_tointeger(L, -1);
		lua_pop(L, 2);
	}
	return index;
}

static int convtable(lua_State *L);

/* 将虚拟机栈位置 index 上的值设置到节点 n 中. 要求值必须是 nil, 数字, 字符串, 布尔值和表中的一种.
 *
 * 参数: ctx 是构建 skynet 表结构时定义的上下文; L 是包含数据的虚拟机栈; index 是值在栈上的位置;
 *      n 是用于保存值的节点; */
static void
setvalue(struct context * ctx, lua_State *L, int index, struct node *n) {
	int vt = lua_type(L, index);
	switch(vt) {
	case LUA_TNIL:
		n->valuetype = VALUETYPE_NIL;
		break;
	case LUA_TNUMBER:
		if (lua_isinteger(L, index)) {
			n->v.d = lua_tointeger(L, index);
			n->valuetype = VALUETYPE_INTEGER;
		} else {
			n->v.n = lua_tonumber(L, index);
			n->valuetype = VALUETYPE_REAL;
		}
		break;
	case LUA_TSTRING: {
		size_t sz = 0;
		const char * str = lua_tolstring(L, index, &sz);
		n->v.string = stringindex(ctx, str, sz);
		n->valuetype = VALUETYPE_STRING;
		break;
	}
	case LUA_TBOOLEAN:
		n->v.boolean = lua_toboolean(L, index);
		n->valuetype = VALUETYPE_BOOLEAN;
		break;
	case LUA_TTABLE: {
		struct table *tbl = ctx->tbl;
		ctx->tbl = (struct table *)malloc(sizeof(struct table));
		if (ctx->tbl == NULL) {
			ctx->tbl = tbl;
			luaL_error(L, "memory error");
			// never get here
		}
		memset(ctx->tbl, 0, sizeof(struct table));
		int absidx = lua_absindex(L, index);

		lua_pushcfunction(L, convtable);
		lua_pushvalue(L, absidx);
		lua_pushlightuserdata(L, ctx);

		lua_call(L, 2, 0);

		n->v.tbl = ctx->tbl;
		n->valuetype = VALUETYPE_TABLE;

		ctx->tbl = tbl;

		break;
	}
	default:
		luaL_error(L, "Unsupport value type %s", lua_typename(L, vt));
		break;
	}
}

/* 将 Lua 表中序列部分的一个值设置到 skynet 表结构的数组中去. 值在栈上 index 的位置, 其在表中的索引为 key.
 * 参数: ctx 是构建 skynet 表结构时定义的上下文; L 是包含值的虚拟机栈; index 为值在栈上的位置;
 *      key 是序列值在表中对应的整数键; */
static void
setarray(struct context *ctx, lua_State *L, int index, int key) {
	struct node n;
	setvalue(ctx, L, index, &n);
	struct table *tbl = ctx->tbl;
	--key;	// base 0
	tbl->arraytype[key] = n.valuetype;
	tbl->array[key] = n.v;
}

/* 判断虚拟机栈上位置 index 的值是否是哈希键, 并设置相应的键的值、哈希值以及键的类型. 如果不是哈希键则为序列中的索引.
 *
 * 参数: ctx 是构建 skynet 表结构时定义的上下文; L 是包含键的虚拟机栈; index 为键在栈上的位置;
 *      出参 key 为键的值, 如果是字符串则 key 为字符串的数字表示, 仅当键是哈希键时才设置;
 *      出参 keyhash 为键的哈希值, 仅当键是哈希键时才设置;
 *      出参 keytype 为键的类型, 仅当键是哈希键时才设置;
 *
 * 返回: 哈希键返回 1 , 序列中的索引则返回 0 . */
static int
ishashkey(struct context * ctx, lua_State *L, int index, int *key, uint32_t *keyhash, int *keytype) {
	int sizearray = ctx->tbl->sizearray;
	int kt = lua_type(L, index);
	if (kt == LUA_TNUMBER) {
		*key = lua_tointeger(L, index);
		if (*key > 0 && *key <= sizearray) {
			return 0;
		}
		*keyhash = (uint32_t)*key;
		*keytype = KEYTYPE_INTEGER;
	} else {
		size_t sz = 0;
		const char * s = lua_tolstring(L, index, &sz);
		*keyhash = calchash(s, sz);
		*key = stringindex(ctx, s, sz);
		*keytype = KEYTYPE_STRING;
	}
	return 1;
}

/* 向 skynet 定义的表结构中填充 Lua 表中的序列和非冲突的哈希键值对. 所谓非冲突是指按照键的哈希值得到在数组中节点
 * 没有被其它键占据, 如果发现已经被占据了就跳过此键, 等到填充冲突哈希键时再插入.
 *
 * 参数: L 是保存 Lua 表的虚拟机栈, 其中 Lua 表在位置 1 处; ctx 是构建 skynet 表结构时定义的上下文; */
static void
fillnocolliding(lua_State *L, struct context *ctx) {
	struct table * tbl = ctx->tbl;
	lua_pushnil(L);
	while (lua_next(L, 1) != 0) {
		int key;
		int keytype;
		uint32_t keyhash;
		if (!ishashkey(ctx, L, -2, &key, &keyhash, &keytype)) {
			setarray(ctx, L, -1, key);
		} else {
			struct node * n = &tbl->hash[keyhash % tbl->sizehash];
			if (n->valuetype == VALUETYPE_NIL) {
				n->key = key;
				n->keytype = keytype;
				n->keyhash = keyhash;
				n->next = -1;
				n->nocolliding = 1;
				setvalue(ctx, L, -1, n);	// set n->v , n->valuetype
			}
		}
		lua_pop(L,1);
	}
}

/* 向 skynet 定义的表结构中填充 Lua 表中的冲突的哈希键值对. 冲突的键值对将被插入到哈希数组中空闲的节点, 并连接到
 * 原先冲突的节点的下一个节点. 发生冲突的两个节点都将标记为冲突.
 *
 * 参数: L 是保存 Lua 表的虚拟机栈, 其中 Lua 表在位置 1 处; ctx 是构建 skynet 表结构时定义的上下文; */
static void
fillcolliding(lua_State *L, struct context *ctx) {
	struct table * tbl = ctx->tbl;
	int sizehash = tbl->sizehash;
	int emptyslot = 0;
	int i;
	lua_pushnil(L);
	while (lua_next(L, 1) != 0) {
		int key;
		int keytype;
		uint32_t keyhash;
		if (ishashkey(ctx, L, -2, &key, &keyhash, &keytype)) {
			struct node * mainpos = &tbl->hash[keyhash % tbl->sizehash];
			assert(mainpos->valuetype != VALUETYPE_NIL);
			if (!(mainpos->keytype == keytype && mainpos->key == key)) {
				// the key has not insert
				struct node * n = NULL;
				for (i=emptyslot;i<sizehash;i++) {
					if (tbl->hash[i].valuetype == VALUETYPE_NIL) {
						n = &tbl->hash[i];
						break;
					}
				}
				assert(n);
				n->next = mainpos->next;
				mainpos->next = n - tbl->hash;
				mainpos->nocolliding = 0;
				n->key = key;
				n->keytype = keytype;
				n->keyhash = keyhash;
				n->nocolliding = 0;
				setvalue(ctx, L, -1, n);	// set n->v , n->valuetype
			}
		}
		lua_pop(L,1);
	}
}

// table need convert
// struct context * ctx
/* [lua_api] 将 Lua 表转化为 skynet 定义的表结构, 转化后的表能够在多个服务之间共享. 转化后的表结构将保存在
 * 第二个参数的 tbl 字段中. 转化表分为两个步骤, 第一步是将序列部分放入表结构中的 array 中, 第二步将哈希键值对
 * 部分放入表结构中的 hash 中. 其中对 hash 中按照键的哈希索引时冲突的节点, 会链接起来. 此函数要求键必须是整数
 * 或者字符串, 要求值必须是数字、字符串、布尔值和表. 如果值是表, 将继续转化成表结构.
 *
 * 参数: table[1] 是待转化的表; lightuserdata[2] 是构建 skynet 表结构时定义的上下文, 用于保存表结构;
 * 函数无返回值, 转化后的结果保存在第二个参数中. */
static int
convtable(lua_State *L) {
	int i;
	struct context *ctx = lua_touserdata(L,2);
	struct table *tbl = ctx->tbl;

	tbl->L = ctx->L;

	/* 未填充前所有值的类型都是 VALUETYPE_NIL */
	int sizearray = lua_rawlen(L, 1);
	if (sizearray) {
		tbl->arraytype = (uint8_t *)malloc(sizearray * sizeof(uint8_t));
		if (tbl->arraytype == NULL) {
			goto memerror;
		}
		for (i=0;i<sizearray;i++) {
			tbl->arraytype[i] = VALUETYPE_NIL;
		}
		tbl->array = (union value *)malloc(sizearray * sizeof(union value));
		if (tbl->array == NULL) {
			goto memerror;
		}
		tbl->sizearray = sizearray;
	}
	int sizehash = countsize(L, sizearray);
	if (sizehash) {
		tbl->hash = (struct node *)malloc(sizehash * sizeof(struct node));
		if (tbl->hash == NULL) {
			goto memerror;
		}
		for (i=0;i<sizehash;i++) {
			tbl->hash[i].valuetype = VALUETYPE_NIL;
			tbl->hash[i].nocolliding = 0;
		}
		tbl->sizehash = sizehash;

		fillnocolliding(L, ctx);
		fillcolliding(L, ctx);
	} else {
		/* 当 Lua 表有哈希键值对时将在 fillnocolliding 中填充序列值. */
		int i;
		for (i=1;i<=sizearray;i++) {
			lua_rawgeti(L, 1, i);
			setarray(ctx, L, -1, i);
			lua_pop(L,1);
		}
	}

	return 0;
memerror:
	return luaL_error(L, "memory error");
}

/* 删除一个 skynet 定义的表结构的. 此函数将依次递归删除值中子表的内存, 最终回收分配给序列和哈希键值对数组的内存. */
static void
delete_tbl(struct table *tbl) {
	int i;
	for (i=0;i<tbl->sizearray;i++) {
		if (tbl->arraytype[i] == VALUETYPE_TABLE) {
			delete_tbl(tbl->array[i].tbl);
		}
	}
	for (i=0;i<tbl->sizehash;i++) {
		if (tbl->hash[i].valuetype == VALUETYPE_TABLE) {
			delete_tbl(tbl->hash[i].v.tbl);
		}
	}
	free(tbl->arraytype);
	free(tbl->array);
	free(tbl->hash);
	free(tbl);
}

/* [lua_api] 将传递给此函数的虚拟机栈 pL 上的位置 1 上的表转为 skynet 定义的表结构. 所有的字符串都将保存在
 * 虚拟机栈 L 上的位置 1 上的表中并返回, 以字符串为键, 对应的数字表示为值. 如果转换过程中遇到了错误将抛出错误.
 *
 * 参数: lightuserdata[1] 是构建 skynet 表结构时定义的上下文; lightuserdata[2] 为包含 Lua 表的虚拟机栈;
 * 返回: 字符串保存的表 */
static int
pconv(lua_State *L) {
	struct context *ctx = lua_touserdata(L,1);
	lua_State * pL = lua_touserdata(L, 2);
	int ret;

	lua_settop(L, 0);

	// init L (may throw memory error)
	// create a table for string map
	lua_newtable(L);

	lua_pushcfunction(pL, convtable);
	lua_pushvalue(pL,1);
	lua_pushlightuserdata(pL, ctx);

	ret = lua_pcall(pL, 2, 0, 0);
	if (ret != LUA_OK) {
		size_t sz = 0;
		const char * error = lua_tolstring(pL, -1, &sz);
		lua_pushlstring(L, error, sz);
		lua_error(L);
		// never get here
	}

	/* [ck]非必要的语句? 因为在紧接着的函数调用中将确保栈空间大小[/ck] */
	luaL_checkstack(L, ctx->string_index + 3, NULL);
	lua_settop(L,1);

	return 1;
}

/* 将构建 skynet 表结构的上下文中的虚拟机栈 L 上的字符串表解开按照数字表示依次放在栈上. 并且将表结构的状态放在栈的
 * 第一个位置. 函数将对栈上不可达的值执行一个全量的垃圾回收. */
static void
convert_stringmap(struct context *ctx, struct table *tbl) {
	lua_State *L = ctx->L;
	lua_checkstack(L, ctx->string_index + LUA_MINSTACK);
	lua_settop(L, ctx->string_index + 1);
	lua_pushvalue(L, 1);
	struct state * s = lua_newuserdata(L, sizeof(*s));
	s->dirty = 0;
	s->ref = 0;
	s->root = tbl;
	lua_replace(L, 1);
	/* [ck]可以将 top 设置为 string_index, 从而 pushvalue(1) 就在正确的位置, 而不需要 replace(-2)?[ck] */
	lua_replace(L, -2);

	lua_pushnil(L);
	// ... stringmap nil
	while (lua_next(L, -2) != 0) {
		int idx = lua_tointeger(L, -1);
		lua_pop(L, 1);
		lua_pushvalue(L, -1);
		lua_replace(L, idx);
	}

	lua_pop(L, 1);

	lua_gc(L, LUA_GCCOLLECT, 0);
}

/* [lua_api] 将 Lua 表构建成 skynet 定义的表结构, 并返回构建好后的表结构. 要求 Lua 表的键必须是整数或者字符串,
 * 值必须是数字、字符串、布尔值或者表, 表中可以嵌套表, 但表不能有环. 构建好之后所有的字符串将保存在表结构中的虚拟机栈 L 中.
 * 虚拟机栈 L 的第一个位置用于保存表结构的状态 state, 从第二个位置开始用于保存字符串, 位置作为字符串的数字表示.
 * 一旦表结构构建成功将不允许部分修改, 而必须是另外构建一个新的表结构并替换当前的表结构.
 *
 * 参数: table[1] 是需要转换的 Lua 表;
 * 返回: 如果转换成功将返回表结构, 失败将抛出错误. */
static int
lnewconf(lua_State *L) {
	int ret;
	struct context ctx;
	struct table * tbl = NULL;
	luaL_checktype(L,1,LUA_TTABLE);
	ctx.L = luaL_newstate();
	ctx.tbl = NULL;
	ctx.string_index = 1;	// 1 reserved for dirty flag
	if (ctx.L == NULL) {
		lua_pushliteral(L, "memory error");
		goto error;
	}
	tbl = (struct table *)malloc(sizeof(struct table));
	if (tbl == NULL) {
		// lua_pushliteral may fail because of memory error, close first.
		lua_close(ctx.L);
		ctx.L = NULL;
		lua_pushliteral(L, "memory error");
		goto error;
	}
	memset(tbl, 0, sizeof(struct table));
	ctx.tbl = tbl;

	lua_pushcfunction(ctx.L, pconv);
	lua_pushlightuserdata(ctx.L , &ctx);
	lua_pushlightuserdata(ctx.L , L);

	ret = lua_pcall(ctx.L, 2, 1, 0);

	if (ret != LUA_OK) {
		size_t sz = 0;
		const char * error = lua_tolstring(ctx.L, -1, &sz);
		lua_pushlstring(L, error, sz);
		goto error;
	}

	convert_stringmap(&ctx, tbl);

	lua_pushlightuserdata(L, tbl);

	return 1;
error:
	if (ctx.L) {
		lua_close(ctx.L);
	}
	if (tbl) {
		delete_tbl(tbl);
	}
	lua_error(L);
	return -1;
}

/* 从虚拟机栈上的 index 位置处获取表结构的指针, 如果指定位置上没有表结构将抛出错误. */
static struct table *
get_table(lua_State *L, int index) {
	struct table *tbl = lua_touserdata(L,index);
	if (tbl == NULL) {
		luaL_error(L, "Need a conf object");
	}
	return tbl;
}

/* [lua_api] 删除一个 skynet 定义的表结构. 函数将关闭表结构关联的虚拟机栈, 并回收表以及子表的内存.
 * 参数: lightuserdata[1] 是待删除的表结构;
 * 函数无返回值 */
static int
ldeleteconf(lua_State *L) {
	struct table *tbl = get_table(L,1);
	lua_close(tbl->L);
	delete_tbl(tbl);
	return 0;
}

/* 将类型为 vt 的值 v 压栈到虚拟机栈 L 上, 如果是字符串, 还将从虚拟机栈 sL 上获取字符串. */
static void
pushvalue(lua_State *L, lua_State *sL, uint8_t vt, union value *v) {
	switch(vt) {
	case VALUETYPE_REAL:
		lua_pushnumber(L, v->n);
		break;
	case VALUETYPE_INTEGER:
		lua_pushinteger(L, v->d);
		break;
	case VALUETYPE_STRING: {
		size_t sz = 0;
		const char *str = lua_tolstring(sL, v->string, &sz);
		lua_pushlstring(L, str, sz);
		break;
	}
	case VALUETYPE_BOOLEAN:
		lua_pushboolean(L, v->boolean);
		break;
	case VALUETYPE_TABLE:
		lua_pushlightuserdata(L, v->tbl);
		break;
	default:
		lua_pushnil(L);
		break;
	}
}

/* 从表结构的哈希数组中查找键, 并返回相应的节点. 函数通过键的哈希值定位到数组中的某个位置, 如果这个位置上链接着多个节点
 * 将依次比较每一个节点. 针对整数键和字符串键有两种不同的比较方法, 整数键将直接比较键值, 而字符串键需要从关联的虚拟机栈
 * 取出字符串并比较. 如果找到将返回此节点, 未找到将返回 NULL.
 *
 * 参数: tbl 是当前查找的表结构; keyhash 是待查找的键的哈希值; key 是键的值, 仅当时整数键时提供;
 *      keytype 是键的类型, 有整数键和字符串键两种; str 是查找的字符串, 仅当字符串键时提供;
 *      sz 是字符串的长度, 仅当字符串键时提供;
 *
 * 返回: 查找到的节点, 或者当查找不到时返回 NULL. */
static struct node *
lookup_key(struct table *tbl, uint32_t keyhash, int key, int keytype, const char *str, size_t sz) {
	if (tbl->sizehash == 0)
		return NULL;
	struct node *n = &tbl->hash[keyhash % tbl->sizehash];
	if (keyhash != n->keyhash && n->nocolliding)
		return NULL;
	for (;;) {
		if (keyhash == n->keyhash) {
			if (n->keytype == KEYTYPE_INTEGER) {
				if (keytype == KEYTYPE_INTEGER && n->key == key) {
					return n;
				}
			} else {
				// n->keytype == KEYTYPE_STRING
				if (keytype == KEYTYPE_STRING) {
					size_t sz2 = 0;
					const char * str2 = lua_tolstring(tbl->L, n->key, &sz2);
					if (sz == sz2 && memcmp(str,str2,sz) == 0) {
						return n;
					}
				}
			}
		}
		if (n->next < 0) {
			return NULL;
		}
		n = &tbl->hash[n->next];		
	}
}

/* [lua_api] 从表结构中按照键查找值, 并返回相应的值. 如果查找到是表将返回表结构的指针, 没有找到将返回 nil.
 * 要求查找的键必须是整数或者字符串.
 *
 * 参数: lightuserdata[1] 从中查找键值的表; int/string 待查找的键;
 * 返回: 如果查找到则为所有合法的值中的一种, 如果查找不到则返回 nil. */
static int
lindexconf(lua_State *L) {
	struct table *tbl = get_table(L,1);
	int kt = lua_type(L,2);
	uint32_t keyhash;
	int key = 0;
	int keytype;
	size_t sz = 0;
	const char * str = NULL;
	if (kt == LUA_TNUMBER) {
		if (!lua_isinteger(L, 2)) {
			return luaL_error(L, "Invalid key %f", lua_tonumber(L, 2));
		}
		key = (int)lua_tointeger(L, 2);
		if (key > 0 && key <= tbl->sizearray) {
			--key;
			pushvalue(L, tbl->L, tbl->arraytype[key], &tbl->array[key]);
			return 1;
		}
		keytype = KEYTYPE_INTEGER;
		keyhash = (uint32_t)key;
	} else {
		str = luaL_checklstring(L, 2, &sz);
		keyhash = calchash(str, sz);
		keytype = KEYTYPE_STRING;
	}

	struct node *n = lookup_key(tbl, keyhash, key, keytype, str, sz);
	if (n) {
		pushvalue(L, tbl->L, n->valuetype, &n->v);
		return 1;
	} else {
		return 0;
	}
}

/* 将节点中的键压栈到虚拟机栈 L 上. 键只能是整数或者字符串. */
static void
pushkey(lua_State *L, lua_State *sL, struct node *n) {
	if (n->keytype == KEYTYPE_INTEGER) {
		lua_pushinteger(L, n->key);
	} else {
		size_t sz = 0;
		const char * str = lua_tolstring(sL, n->key, &sz);
		lua_pushlstring(L, str, sz);
	}
}

/* 将表结构的哈希数组中第一个节点的哈希键压栈并返回 1 , 如果不存在哈希数组将不压栈并返回 0 . */
static int
pushfirsthash(lua_State *L, struct table * tbl) {
	if (tbl->sizehash) {
		pushkey(L, tbl->L, &tbl->hash[0]);
		return 1;
	} else {
		return 0;
	}
}

/* [lua_api] 获取表结构中的下一个键并返回, 函数首先返回序列中的索引, 再返回哈希数组中的键. 全部遍历完之后将返回 nil.
 * 两种类型的键的返回顺序都是按照数组的下标来访问的. 第一次访问时, 传入的当前键为 nil.
 *
 * 参数: lightuserdata[1] 是表结构用于遍历键; nil/int/string[2] 是当前的键, 用于查找其下一个键;
 * 返回: 当查询到时返回键的值, 未查询到时返回 nil. */
static int
lnextkey(lua_State *L) {
	struct table *tbl = get_table(L,1);
	if (lua_isnoneornil(L,2)) {
		if (tbl->sizearray > 0) {
			int i;
			for (i=0;i<tbl->sizearray;i++) {
				if (tbl->arraytype[i] != VALUETYPE_NIL) {
					lua_pushinteger(L, i+1);
					return 1;
				}
			}
		}
		return pushfirsthash(L, tbl);
	}
	int kt = lua_type(L,2);
	uint32_t keyhash;
	int key = 0;
	int keytype;
	size_t sz=0;
	const char *str = NULL;
	int sizearray = tbl->sizearray;
	if (kt == LUA_TNUMBER) {
		if (!lua_isinteger(L, 2)) {
			return 0;
		}
		key = (int)lua_tointeger(L, 2);
		if (key > 0 && key <= sizearray) {
			lua_Integer i;
			for (i=key;i<sizearray;i++) {
				if (tbl->arraytype[i] != VALUETYPE_NIL) {
					lua_pushinteger(L, i+1);
					return 1;
				}
			}
			return pushfirsthash(L, tbl);
		}
		keyhash = (uint32_t)key;
		keytype = KEYTYPE_INTEGER;
	} else {
		str = luaL_checklstring(L, 2, &sz);
		keyhash = calchash(str, sz);
		keytype = KEYTYPE_STRING;
	}

	struct node *n = lookup_key(tbl, keyhash, key, keytype, str, sz);
	if (n) {
		++n;
		int index = n-tbl->hash;
		if (index == tbl->sizehash) {
			return 0;
		}
		pushkey(L, tbl->L, n);
		return 1;
	} else {
		return 0;
	}
}

/* [lua_api] 获取表结构中序列部分的长度.
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: int[1] 是序列部分的长度 */
static int
llen(lua_State *L) {
	struct table *tbl = get_table(L,1);
	lua_pushinteger(L, tbl->sizearray);
	return 1;
}

/* [lua_api] 获取表结构中哈希部分的长度.
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: int[1] 是哈希部分的长度 */
static int
lhashlen(lua_State *L) {
	struct table *tbl = get_table(L,1);
	lua_pushinteger(L, tbl->sizehash);
	return 1;
}

/* [lua_api] 作为客户端表结构包装对象的垃圾回收函数. 当创建包装对象时会增加表结构的引用, 此函数将减小此引用.
 * 参数: userdata[1] 是待回收的用户数据; */
static int
releaseobj(lua_State *L) {
	struct ctrl *c = lua_touserdata(L, 1);
	struct table *tbl = c->root;
	struct state *s = lua_touserdata(tbl->L, 1);
	ATOM_DEC(&s->ref);
	c->root = NULL;
	c->update = NULL;

	return 0;
}

/* [lua_api] 生成一个客户端表结构包装对象的完全用户数据, 调用此函数将会增加表结构的引用. 函数还将为包装对象设置 __gc 函数.
 * 当包装对象回收时将减小表结构的引用. 一旦生成, root 表结构将不会更改为别的表.
 *
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: userdata[1] 是生成的表结构包装对象. */
static int
lboxconf(lua_State *L) {
	struct table * tbl = get_table(L,1);
	struct state * s = lua_touserdata(tbl->L, 1);
	ATOM_INC(&s->ref);

	struct ctrl * c = lua_newuserdata(L, sizeof(*c));
	c->root = tbl;
	c->update = NULL;
	if (luaL_newmetatable(L, "confctrl")) {
		lua_pushcfunction(L, releaseobj);
		lua_setfield(L, -2, "__gc");
	}
	lua_setmetatable(L, -2);

	return 1;
}

/* [lua_api] 当更新了表结构之后, 将当前的表结构标记为脏数据. 当表结构发生更新时, 将会标记此表结构为脏数据.
 * 参数: lightuserdata[1] 为 skynet 定义的表结构; */
static int
lmarkdirty(lua_State *L) {
	struct table *tbl = get_table(L,1);
	struct state * s = lua_touserdata(tbl->L, 1);
	s->dirty = 1;
	return 0;
}

/* [lua_api] 客户端检查 skynet 定义的表结构是否为脏数据. 当表结构发生更新时, 将会标记此表结构为脏数据.
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: boolean[1] 脏数据将返回 true, 干净的数据将返回 false. */
static int
lisdirty(lua_State *L) {
	struct table *tbl = get_table(L,1);
	struct state * s = lua_touserdata(tbl->L, 1);
	int d = s->dirty;
	lua_pushboolean(L, d);
	
	return 1;
}

/* [lua_api] 获取表结构的引用数量. 每次客户端包装表结构时引用都会增加, 生成新的表结构时旧的表结构的引用会减少.
 * 而刚生成的表结构自动拥有一个引用, 删除表结构则是将引用减一. 当引用为 0 时, 将会调用函数 ldeleteconf 删除此表结构.
 *
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: int[1] 是表结构的当前引用数 */
static int
lgetref(lua_State *L) {
	struct table *tbl = get_table(L,1);
	struct state * s = lua_touserdata(tbl->L, 1);
	lua_pushinteger(L , s->ref);

	return 1;
}

/* [lua_api] 增加表结构的引用数量. 这会发生在客户端查询此表结构, 服务端生成表结构时.
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: int[1] 为自增后的引用数. */
static int
lincref(lua_State *L) {
	struct table *tbl = get_table(L,1);
	struct state * s = lua_touserdata(tbl->L, 1);
	int ref = ATOM_INC(&s->ref);
	lua_pushinteger(L , ref);

	return 1;
}

/* [lua_api] 减小表结构的引用数量. 这会发生在客户端确认(confirm)此表结构, 服务端删除表结构时.
 * 参数: lightuserdata[1] 为 skynet 定义的表结构;
 * 返回: int[1] 为自减后的引用数. */
static int
ldecref(lua_State *L) {
	struct table *tbl = get_table(L,1);
	struct state * s = lua_touserdata(tbl->L, 1);
	int ref = ATOM_DEC(&s->ref);
	lua_pushinteger(L , ref);

	return 1;
}

/* [lua_api] 从已经更新的表结构中获取新的表结构以及关联值. 如果未更新将返回 nil.
 * 参数: userdata[1] 是表结构包装对象;
 * 返回: lightuserdata[1] 为更新后的表结构; table 为关联在包装对象上的关联值; 当未更新时返回 nil. */
static int
lneedupdate(lua_State *L) {
	struct ctrl * c = lua_touserdata(L, 1);
	if (c->update) {
		lua_pushlightuserdata(L, c->update);
		lua_getuservalue(L, 1);
		return 2;
	}
	return 0;
}

/* [lua_api] 更新表结构, 并且在包装对象上设置关联值. 如果更新的表结构是原来的, 将抛出错误.
 *
 * 参数: userdata[1] 是包装对象; lightuserdata[2] 是更新的表结构用于替代原先的表结构;
 *      table[3] 为关联在包装对象上的值, 将为 lneedupdate 取出; */
static int
lupdate(lua_State *L) {
	luaL_checktype(L, 1, LUA_TUSERDATA);
	luaL_checktype(L, 2, LUA_TLIGHTUSERDATA);
	luaL_checktype(L, 3, LUA_TTABLE);
	struct ctrl * c = lua_touserdata(L, 1);
	struct table *n = lua_touserdata(L, 2);
	if (c->root == n) {
		return luaL_error(L, "You should update a new object");
	}
	lua_settop(L, 3);
	lua_setuservalue(L, 1);
	c->update = n;

	return 0;
}

int
luaopen_sharedata_core(lua_State *L) {
	luaL_Reg l[] = {
		// used by host
		{ "new", lnewconf },
		{ "delete", ldeleteconf },
		{ "markdirty", lmarkdirty },
		{ "getref", lgetref },
		{ "incref", lincref },
		{ "decref", ldecref },

		// used by client
		{ "box", lboxconf },
		{ "index", lindexconf },
		{ "nextkey", lnextkey },
		{ "len", llen },
		{ "hashlen", lhashlen },
		{ "isdirty", lisdirty },
		{ "needupdate", lneedupdate },
		{ "update", lupdate },
		{ NULL, NULL },
	};
	luaL_checkversion(L);
	luaL_newlib(L, l);

	return 1;
}
