#include <lua.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#include "rwlock.h"
#include "skynet_malloc.h"
#include "atomic.h"

/* 软件事务内存对象, Software Transactional Memory , 对它读写时需要加上读写锁, 并且需要管理引用.
 * 此对象将包含在写对象和读对象中, 其中写对象只能有一个, 读对象可以有多个 */
struct stm_object {
	struct rwlock lock;      /* 读写锁, 写对象更新消息或者释放对象都需要先加写锁,
	                          * 读对象读取消息或者释放对象都需要加读锁 */
	int reference;           /* 对象引用, 任何一个读写对象都持有一个引用 */
	struct stm_copy * copy;  /* 拷贝, 任何一个读写对象都持有一个引用 */
};

/* stm 对象中的拷贝, 以引用方式决定由读对象还是写对象最终释放拷贝. 任何一个读写对象都持有一个引用.
 * 模块要求 msg 字段的内存必须是堆内存. */
struct stm_copy {
	int reference;      /* 拷贝引用, 占用此拷贝的读写对象都持有其引用 */
	uint32_t sz;        /* 拷贝大小 */
	void * msg;         /* 拷贝消息 */
};

// msg should alloc by skynet_malloc
/* 用消息 msg 和其大小 sz 生成一个信息的拷贝. 此函数只能由写对象调用, 并且生成的拷贝已经有一个引用了. */
static struct stm_copy *
stm_newcopy(void * msg, int32_t sz) {
	struct stm_copy * copy = skynet_malloc(sizeof(*copy));
	copy->reference = 1;
	copy->sz = sz;
	copy->msg = msg;

	return copy;
}

/* 生成一个 stm 对象以及对应的消息拷贝. 此函数只能在生成写对象时调用, 并且每个写对象只能有一个 stm 对象.
 * 调用此函数将持有 stm 对象的一个引用. 并最终由写对象调用 stm_release 函数释放 stm 对象和拷贝的写引用.
 * 函数要求消息 msg 必须是堆内存. */
static struct stm_object *
stm_new(void * msg, int32_t sz) {
	struct stm_object * obj = skynet_malloc(sizeof(*obj));
	rwlock_init(&obj->lock);
	obj->reference = 1;
	obj->copy = stm_newcopy(msg, sz);

	return obj;
}

/* 对 copy 对象进行解引用, 传入的 copy 参数可以是 NULL, 此时将不做任何事. 函数是线程安全的.
 * 此函数可以在调用 stm_copy 之后调用, 也可以被写对象的释放函数调用. */
static void
stm_releasecopy(struct stm_copy *copy) {
	if (copy == NULL)
		return;
	if (ATOM_DEC(&copy->reference) == 0) {
		skynet_free(copy->msg);
		skynet_free(copy);
	}
}

/* 写对象释放 stm 对象. stm 对象将被加上写锁, 并且对拷贝和 stm 对象解除引用, 并导致 stm 对象失去拷贝对象.
 * 如果最终拷贝或者 stm 对象的引用已经为 0 了, 将释放对象的内存. */
static void
stm_release(struct stm_object *obj) {
	assert(obj->copy);
	rwlock_wlock(&obj->lock);
	// writer release the stm object, so release the last copy .
	stm_releasecopy(obj->copy);
	obj->copy = NULL;
	if (--obj->reference > 0) {
		// stm object grab by readers, reset the copy to NULL.
		rwlock_wunlock(&obj->lock);
		return;
	}
	// no one grab the stm object, no need to unlock wlock.
	skynet_free(obj);
}

/* 读对象释放 stm 对象. stm 对象将被加上读锁, 并且对 stm 对象解除引用. 如果最终 stm 对象的引用为 0 说明写
 * 对象也释放了 stm 对象, 此时对象的拷贝将为 NULL, 此时释放对象的内存. 此函数与 stm_grab 是一对. */
static void
stm_releasereader(struct stm_object *obj) {
	rwlock_rlock(&obj->lock);
	if (ATOM_DEC(&obj->reference) == 0) {
		// last reader, no writer. so no need to unlock
		assert(obj->copy == NULL);
		skynet_free(obj);
		return;
	}
	rwlock_runlock(&obj->lock);
}

/* 读对象持有 stm 对象. stm 对象将被加上读锁. 函数将导致 stm 对象的引用增加 1 . 此函数与 stm_releasereader
 * 是一对. */
static void
stm_grab(struct stm_object *obj) {
	rwlock_rlock(&obj->lock);
	int ref = ATOM_FINC(&obj->reference);
	rwlock_runlock(&obj->lock);
	assert(ref > 0);
}

/* 读对象持有拷贝对象. stm 对象将被加上读锁. 函数将导致拷贝对象的引用增加 1 . 此函数与 stm_releasecopy 是一对. */
static struct stm_copy *
stm_copy(struct stm_object *obj) {
	rwlock_rlock(&obj->lock);
	struct stm_copy * ret = obj->copy;
	if (ret) {
		int ref = ATOM_FINC(&ret->reference);
		assert(ref > 0);
	}
	rwlock_runlock(&obj->lock);
	
	return ret;
}

/* 更新 stm 对象中的消息. 函数将使用消息 msg 和其大小 sz 生成新的拷贝对象. 并解除原来的拷贝的引用, 而替换为新的拷贝. */
static void
stm_update(struct stm_object *obj, void *msg, int32_t sz) {
	struct stm_copy *copy = stm_newcopy(msg, sz);
	rwlock_wlock(&obj->lock);
	struct stm_copy *oldcopy = obj->copy;
	obj->copy = copy;
	rwlock_wunlock(&obj->lock);

	stm_releasecopy(oldcopy);
}

// lua binding
/* stm 对象的写对象 */
struct boxstm {
	struct stm_object * obj;
};

/* [lua_api] 从写对象中增持一个 stm 对象的引用, 并返回 stm 对象指针给读对象保存. 返回的 stm 对象指针
 * 用于下一步调用 lnewreader 函数.
 *
 * 参数: userdata[1] 为写对象;
 * 返回: lightuserdata[1] 增加引用后的 stm 对象指针; */
static int
lcopy(lua_State *L) {
	struct boxstm * box = lua_touserdata(L, 1);
	stm_grab(box->obj);
	lua_pushlightuserdata(L, box->obj);
	return 1;
}

/* [lua_api] 生成一个写对象. 参数可以是二进制数据和其大小, 也可以是字符串对象. 二进制数据必须在堆内存中,
 * 字符串对象也会转化为堆内存数据. 函数将生成一个 stm 对象并将其包装到写对象中. 写对象的垃圾回收元方法是
 * ldeletewriter 能够自动解除 stm 对象和拷贝对象的引用. 生成的写对象能够作为函数使用, 并以二进制数据
 * 或者字符串为参数更新 stm 对象中的消息.
 *
 * 参数: userdata[1], int[2] 分别是消息和消息大小; string[1] 是字符串格式的消息;
 * 返回: userdata[1] 为写对象, 能够作为函数使用. */
static int
lnewwriter(lua_State *L) {
	void * msg;
	size_t sz;
	if (lua_isuserdata(L,1)) {
		msg = lua_touserdata(L, 1);
		sz = (size_t)luaL_checkinteger(L, 2);
	} else {
		const char * tmp = luaL_checklstring(L,1,&sz);
		msg = skynet_malloc(sz);
		memcpy(msg, tmp, sz);
	}
	struct boxstm * box = lua_newuserdata(L, sizeof(*box));
	box->obj = stm_new(msg,sz);
	lua_pushvalue(L, lua_upvalueindex(1));
	lua_setmetatable(L, -2);

	return 1;
}

/* 写对象的垃圾回收元方法, 当写对象不可达时以此对象为唯一参数, 释放 stm 对象和拷贝对象的引用. */
static int
ldeletewriter(lua_State *L) {
	struct boxstm * box = lua_touserdata(L, 1);
	stm_release(box->obj);
	box->obj = NULL;

	return 0;
}

/* [lua_api] 写对象的 __call 元方法. 将以写对象为第一个参数, 以二进制消息或者字符串为第二个参数,
 * 如果参数是二进制消息那么还以消息的大小为第三个参数. 函数将使用消息生成新的拷贝对象. 并解除原来的
 * 拷贝的引用, 而替换为新的拷贝. */
static int
lupdate(lua_State *L) {
	struct boxstm * box = lua_touserdata(L, 1);
	void * msg;
	size_t sz;
	if (lua_isuserdata(L, 2)) {
		msg = lua_touserdata(L, 2);
		sz = (size_t)luaL_checkinteger(L, 3);
	} else {
		const char * tmp = luaL_checklstring(L,2,&sz);
		msg = skynet_malloc(sz);
		memcpy(msg, tmp, sz);
	}
	stm_update(box->obj, msg, sz);

	return 0;
}

/* stm 对象的读对象, obj 需要事先使用 lcopy 获取并增持 stm 对象引用. lastcopy 则需要调用
 * lread 函数获取并增持拷贝对象的引用. */
struct boxreader {
	struct stm_object *obj;
	struct stm_copy *lastcopy;
};

/* [lua_api] 生成一个新的读对象, 在调用此函数前必须已经调用 lcopy 获取 stm 对象并增加其引用.
 * 读对象的垃圾回收元方法是 ldeletereader 能够自动解除引用 stm 对象和拷贝对象. 可以将此读对象
 * 作为函数使用, 调用时需要传入一个解包数据函数, 并最终得到解包后的数据. 此函数对于任何传入的
 * stm 对象只能调用一次生成新的读对象, 否则将在回收时引发多次解引用. 调用此方法并不持有拷贝对象.
 *
 * 参数: lightuserdata[1] 为 stm 对象指针;
 * 返回: userdata[1] 是读对象, 可以作为函数调用; */
static int
lnewreader(lua_State *L) {
	struct boxreader * box = lua_newuserdata(L, sizeof(*box));
	box->obj = lua_touserdata(L, 1);
	box->lastcopy = NULL;
	lua_pushvalue(L, lua_upvalueindex(1));
	lua_setmetatable(L, -2);

	return 1;
}

/* 读对象的垃圾回收元方法. 当读对象不再可达时以此对象为唯一参数, 对 stm 对象和 copy 对象进行解引用. */
static int
ldeletereader(lua_State *L) {
	struct boxreader * box = lua_touserdata(L, 1);
	stm_releasereader(box->obj);
	box->obj = NULL;
	stm_releasecopy(box->lastcopy);
	box->lastcopy = NULL;

	return 0;
}

/* [lua_api] 作为读对象的 __call 元方法, 调用时第一个传入的参数是读对象, 第二个参数是解数据包函数, 以及可选的
 * 第三个参数. 最终将调用此解包函数, 将数据解包成 Lua 值返回, 第一个值为 true 表示解包成功. 否则将返回 false.
 * 解包函数的签名是 unpack(msg, sz, parm) , 第三个参数 parm 是可选的. 注意此函数对于相同的 stm 数据只能读取
 * 一次, 第二次读取将返回 false.
 *
 * 参数: userdata[1] 是读对象本身; function[2] 是解包函数; [3] 可选的第三个参数用于传递给解包函数;
 * 返回: boolean[1] 是否解包成功, 紧跟着解包出来的数据. */
static int
lread(lua_State *L) {
	struct boxreader * box = lua_touserdata(L, 1);
	luaL_checktype(L, 2, LUA_TFUNCTION);

	struct stm_copy * copy = stm_copy(box->obj);
	if (copy == box->lastcopy) {
		// not update
		stm_releasecopy(copy);
		lua_pushboolean(L, 0);
		return 1;
	}

	stm_releasecopy(box->lastcopy);
	box->lastcopy = copy;
	if (copy) {
		/* 可选的第三个参数会作为解包函数的第三个参数 */
		lua_settop(L, 3);
		lua_replace(L, 1);
		lua_settop(L, 2);
		lua_pushlightuserdata(L, copy->msg);
		lua_pushinteger(L, copy->sz);
		lua_pushvalue(L, 1);
		lua_call(L, 3, LUA_MULTRET);
		lua_pushboolean(L, 1);
		lua_replace(L, 1);
		return lua_gettop(L);
	} else {
		lua_pushboolean(L, 0);
		return 1;
	}
}

/* 将 stm 相关的函数注册到 Lua 中去. lcopy 函数注册是没有上值, 而 lnewwriter 和 lnewreader 则分别有它们的上值. */
int
luaopen_stm(lua_State *L) {
	luaL_checkversion(L);
	lua_createtable(L, 0, 3);

	lua_pushcfunction(L, lcopy);
	lua_setfield(L, -2, "copy");

	luaL_Reg writer[] = {
		{ "new", lnewwriter },
		{ NULL, NULL },
	};
	lua_createtable(L, 0, 2);
	lua_pushcfunction(L, ldeletewriter),
	lua_setfield(L, -2, "__gc");
	lua_pushcfunction(L, lupdate),
	lua_setfield(L, -2, "__call");
	luaL_setfuncs(L, writer, 1);

	luaL_Reg reader[] = {
		{ "newcopy", lnewreader },
		{ NULL, NULL },
	};
	lua_createtable(L, 0, 2);
	lua_pushcfunction(L, ldeletereader),
	lua_setfield(L, -2, "__gc");
	lua_pushcfunction(L, lread),
	lua_setfield(L, -2, "__call");
	luaL_setfuncs(L, reader, 1);

	return 1;
}
