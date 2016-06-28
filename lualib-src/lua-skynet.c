#include "skynet.h"
#include "lua-seri.h"

#define KNRM  "\x1B[0m"        /* 终端个性化颜色设置的结束命令 */
#define KRED  "\x1B[31m"       /* 终端个性化颜色设置的开始命令, 选定颜色为红色 */

#include <lua.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

struct snlua {
	lua_State * L;
	struct skynet_context * ctx;
	const char * preload;
};

/* [lua_api] 以错误消息为唯一参数, 获取当前的栈回溯信息, 将此消息附加到栈回溯信息之前. 返回这个结果.
 * 如果不提供消息将不获取栈回溯信息而仅返回消息指示无错误消息.
 *
 * 参数: string [1] 是错误消息;
 * 返回: 错误消息附加的栈回溯信息. */
static int
traceback (lua_State *L) {
	const char *msg = lua_tostring(L, 1);
	if (msg)
		luaL_traceback(L, L, msg, 1);
	else {
		lua_pushliteral(L, "(no error message)");
	}
	return 1;
}

/* Lua 服务的回调函数, 将在服务初始化时通过调用 _callback 函数设置到服务的 cb 字段中去. 这个函数将调用注册表中的回调函数完成消息处理工作.
 * 当注册表中的回调函数调用成功将直接返回 0, 否则将发送错误日志并返回 0. 0 在当前语境下表示调用成功, 从而无论 Lua 服务中发生了任何错误都不会传播到系统底层.
 * 注册表中的回调函数的签名是 callback(type, msg, sz, session, source) , 不需要返回值.
 *
 * 参数: context 为处理消息的服务, 也是此回调函数的所属服务; ud 是回调函数的用户数据, 在当前语境下为 Lua 服务虚拟机的主线程;
 *       type 是消息的类型, 参考 skynet.h 中的消息类型; session 是当前消息的会话号, 服务接收到的每个消息都有一个唯一的会话号, 一个会话号表示一次消息调用;
 *       msg 是消息体; sz 是消息的大小;
 * 返回: 0 表示调用成功, 函数未释放消息的内存; 1 表示调用失败, 函数释放了消息的内存; */
static int
_cb(struct skynet_context * context, void * ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	lua_State *L = ud;
	int trace = 1;
	int r;
	/* 第一次调用完之后, 错误处理函数和回调函数已经在栈上了 */
	int top = lua_gettop(L);
	if (top == 0) {
		lua_pushcfunction(L, traceback);
		lua_rawgetp(L, LUA_REGISTRYINDEX, _cb);
	} else {
		assert(top == 2);
	}
	lua_pushvalue(L,2);

	lua_pushinteger(L, type);
	lua_pushlightuserdata(L, (void *)msg);
	lua_pushinteger(L,sz);
	lua_pushinteger(L, session);
	lua_pushinteger(L, source);

	r = lua_pcall(L, 5, 0 , trace);

	if (r == LUA_OK) {
		return 0;
	}
	const char * self = skynet_command(context, "REG", NULL);
	switch (r) {
	case LUA_ERRRUN:
		skynet_error(context, "lua call [%x to %s : %d msgsz = %d] error : " KRED "%s" KNRM, source , self, session, sz, lua_tostring(L,-1));
		break;
	case LUA_ERRMEM:
		skynet_error(context, "lua memory error : [%x to %s : %d]", source , self, session);
		break;
	case LUA_ERRERR:
		skynet_error(context, "lua error in error : [%x to %s : %d]", source , self, session);
		break;
	case LUA_ERRGCMM:
		skynet_error(context, "lua gc error : [%x to %s : %d]", source , self, session);
		break;
	};

	lua_pop(L,1);

	return 0;
}

/* Lua 服务的回调函数, 功能与 _cb 一样. 但是返回值为 1 指示分发消息的函数不需要释放消息的内存. */
static int
forward_cb(struct skynet_context * context, void * ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	_cb(context, ud, type, session, source, msg, sz);
	// don't delete msg in forward mode.
	return 1;
}

/* [lua_api] 将一个 Lua 函数设置为当前服务的回调函数. 这个回调函数会在每次服务处理消息时被调用.
 * Lua 回调函数的签名是  callback(type, msg, sz, session, source) , 回调函数不需要返回值.
 * 当不提供第二个参数时设置的回调函数在每次消息处理完之后都会指示分发函数释放消息的内存, 而当提供第二个参数时将指示分发函数不必释放消息内存.
 *
 * 参数: function [1] 是回调函数; boolean [2] 表示是否使用 forward 模式; */
static int
lcallback(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	int forward = lua_toboolean(L, 2);
	luaL_checktype(L,1,LUA_TFUNCTION);
	lua_settop(L,1);
	lua_rawsetp(L, LUA_REGISTRYINDEX, _cb);

	lua_rawgeti(L, LUA_REGISTRYINDEX, LUA_RIDX_MAINTHREAD);
	lua_State *gL = lua_tothread(L,-1);

	if (forward) {
		skynet_callback(context, gL, forward_cb);
	} else {
		skynet_callback(context, gL, _cb);
	}

	return 0;
}

/* [lua_api] 调用 skynet 内置的命令, 命令有 TIMEOUT, REG, QUERY, NAME, EXIT, KILL, LAUNCH, GETENV, SETENV, STARTTIME, ENDLESS,
 * ABORT, MONITOR, MQLEN, LOGON, LOGOFF, SIGNAL .
 * 具体参考 skynet_server.c 的 cmd_funcs 数组, 命令的参数都是字符串形式, 有的命令没有参数. 命令是区分大小写的, 如果发起不存在的命令将返回 nil .
 *
 * 参数: string [1] 是命令字符串; string [2] 如果存在则为命令的参数;
 * 返回: string/nil [1] 命令执行的结果, 或者无任何返回值 */
static int
lcommand(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	const char * cmd = luaL_checkstring(L,1);
	const char * result;
	const char * parm = NULL;
	if (lua_gettop(L) == 2) {
		parm = luaL_checkstring(L,2);
	}

	result = skynet_command(context, cmd, parm);
	if (result) {
		lua_pushstring(L, result);
		return 1;
	}
	return 0;
}

/* [lua_api] 以 int 类型值为参数调用 skynet 内置的命令. 命令有 TIMEOUT, REG, QUERY, NAME, EXIT, KILL, LAUNCH, GETENV, SETENV, STARTTIME, ENDLESS,
 * ABORT, MONITOR, MQLEN, LOGON, LOGOFF, SIGNAL .
 * 具体参考 skynet_server.c 的 cmd_funcs 数组, 内置命令的参数都是字符串类型, 再调用之前会将整数转为字符串类型. 得到的结果也会转为整数类型.
 *
 * 参数: string [1] 是命令字符串; int [2] 如果存在则为命令的参数;
 * 返回: int/nil [1] 命令执行的结果, 或者无任何返回值 */
static int
lintcommand(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	const char * cmd = luaL_checkstring(L,1);
	const char * result;
	const char * parm = NULL;
	char tmp[64];	// for integer parm
	if (lua_gettop(L) == 2) {
		int32_t n = (int32_t)luaL_checkinteger(L,2);
		sprintf(tmp, "%d", n);
		parm = tmp;
	}

	result = skynet_command(context, cmd, parm);
	if (result) {
		lua_Integer r = strtoll(result, NULL, 0);
		lua_pushinteger(L, r);
		return 1;
	}
	return 0;
}

/* [lua_api] 在当前服务上分配一个会话号. 函数无参数, 返回整形的会话号. */
static int
lgenid(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	/* 目标服务句柄为 0 将不发送消息, 而仅仅是分配一个会话号 */
	int session = skynet_send(context, 0, 0, PTYPE_TAG_ALLOCSESSION , 0 , NULL, 0);
	lua_pushinteger(L, session);
	return 1;
}

/* 将 Lua 虚拟机栈上位置为 index 的值转换为字符串, 要求 index 位置处的值必须是字符串或者数字. 如果类型不正确将抛出错误.
 * 返回: 转化后的目标服务字符串 */
static const char *
get_dest_string(lua_State *L, int index) {
	const char * dest_string = lua_tostring(L, index);
	if (dest_string == NULL) {
		luaL_error(L, "dest address type (%s) must be a string or number.", lua_typename(L, lua_type(L,index)));
	}
	return dest_string;
}

/*
	uint32 address
	 string address
	integer type
	integer session
	string message
	 lightuserdata message_ptr
	 integer len
 */
 /* [lua_api] 向别的服务发送消息. 目标服务地址可以是整数形式的句柄, 也可以是字符串形式的服务名字. 如果不提供会话号, 将会告知 skynet 系统分配.
  * 消息可以是字符串类型也可以是用户数据, 如果是轻量用户数据, 还需要提供数据大小. 轻量用户数据内存的释放将交由 skynet 底层完成, 因而不需要在 Lua 释放.
  * 任何其它类型的消息格式都将抛出异常.
  *
  * 参数: int/string [1] 表示服务句柄或者服务名字; int [2] 是消息类型, 定义在 skynet.lua 中; int/nil [3] 是会话号, 如果是 nil 将有系统分配;
  *       string/lightuserdata [4] 是消息数据; int [5] 仅在消息是轻量用户数据时提供, 用于指示数据的长度;
  *
  * 返回: 发送成功后的会话号, 或者发送失败时返回 nil, 当参数不符合要求时将抛出错误 */
static int
lsend(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	uint32_t dest = (uint32_t)lua_tointeger(L, 1);
	const char * dest_string = NULL;
	if (dest == 0) {
		if (lua_type(L,1) == LUA_TNUMBER) {
			return luaL_error(L, "Invalid service address 0");
		}
		dest_string = get_dest_string(L, 1);
	}

	int type = luaL_checkinteger(L, 2);
	int session = 0;
	if (lua_isnil(L,3)) {
		type |= PTYPE_TAG_ALLOCSESSION;
	} else {
		session = luaL_checkinteger(L,3);
	}

	int mtype = lua_type(L,4);
	switch (mtype) {
	case LUA_TSTRING: {
		size_t len = 0;
		void * msg = (void *)lua_tolstring(L,4,&len);
		if (len == 0) {
			msg = NULL;
		}
		if (dest_string) {
			session = skynet_sendname(context, 0, dest_string, type, session , msg, len);
		} else {
			session = skynet_send(context, 0, dest, type, session , msg, len);
		}
		break;
	}
	/* 必须是 LUA_TLIGHTUSERDATA 的原因在于, 完全用户数据的内存由 Lua 管理, skynet 底层不能释放它的内存 */
	case LUA_TLIGHTUSERDATA: {
		void * msg = lua_touserdata(L,4);
		int size = luaL_checkinteger(L,5);
		if (dest_string) {
			session = skynet_sendname(context, 0, dest_string, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		} else {
			session = skynet_send(context, 0, dest, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		}
		break;
	}
	default:
		luaL_error(L, "skynet.send invalid param %s", lua_typename(L, lua_type(L,4)));
	}
	if (session < 0) {
		// send to invalid address
		// todo: maybe throw an error would be better
		return 0;
	}
	lua_pushinteger(L,session);
	return 1;
}

/* [lua_api] 将收到的消息转发到新的目标服务. 目标服务地址可以是整数形式的句柄, 也可以是字符串形式的服务名字. 参数还必须提供来源服务地址和会话号.
 * 消息可以是字符串类型也可以是用户数据, 如果是轻量用户数据, 还需要提供数据大小. 轻量用户数据内存的释放将交由 skynet 底层完成, 因而不需要在 Lua 释放.
 * 调用此函数的回调函数应该是 forward 模式, 否则将有可能释放两次轻量用户数据内存, 或者以字符串类型转发数据.
 *
 * 参数: int/string[1] 目标服务句柄; int[2] 来源服务句柄; int[3] 是消息类型; int[4] 是会话号; string/lightuserdata[5] 消息数据;
 *       int[6] 仅在消息是轻量用户数据时提供, 用于指示数据的长度;
 *
 * 返回: 当参数格式不对时将抛出错误, 正常情况下无任何返回值 */
static int
lredirect(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	uint32_t dest = (uint32_t)lua_tointeger(L,1);
	const char * dest_string = NULL;
	if (dest == 0) {
		dest_string = get_dest_string(L, 1);
	}
	uint32_t source = (uint32_t)luaL_checkinteger(L,2);
	int type = luaL_checkinteger(L,3);
	int session = luaL_checkinteger(L,4);

	int mtype = lua_type(L,5);
	switch (mtype) {
	case LUA_TSTRING: {
		size_t len = 0;
		void * msg = (void *)lua_tolstring(L,5,&len);
		if (len == 0) {
			msg = NULL;
		}
		if (dest_string) {
			session = skynet_sendname(context, source, dest_string, type, session , msg, len);
		} else {
			session = skynet_send(context, source, dest, type, session , msg, len);
		}
		break;
	}
	case LUA_TLIGHTUSERDATA: {
		void * msg = lua_touserdata(L,5);
		int size = luaL_checkinteger(L,6);
		if (dest_string) {
			session = skynet_sendname(context, source, dest_string, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		} else {
			session = skynet_send(context, source, dest, type | PTYPE_TAG_DONTCOPY, session, msg, size);
		}
		break;
	}
	default:
		luaL_error(L, "skynet.redirect invalid param %s", lua_typename(L,mtype));
	}
	return 0;
}

/* [lua_api] 向日志服务 logger 发送日志. 唯一参数是日志消息. */
static int
lerror(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	int n = lua_gettop(L);
	if (n <= 1) {
		lua_settop(L, 1);
		const char * s = luaL_tolstring(L, 1, NULL);
		skynet_error(context, "%s", s);
		return 0;
	}
	luaL_Buffer b;
	luaL_buffinit(L, &b);
	int i;
	for (i=1; i<=n; i++) {
		luaL_tolstring(L, i, NULL);
		luaL_addvalue(&b);
		if (i<n) {
			luaL_addchar(&b, ' ');
		}
	}
	luaL_pushresult(&b);
	skynet_error(context, "%s", lua_tostring(L, -1));
	return 0;
}

/* [lua_api] 将一个用户数据转化为 Lua 中的字符串. 如果没有提供参数将返回 nil.
 * 参数: lightuserdata[1] 是数据内存块的起始地址; int[2] 是内存块的大小;
 * 返回: 转化后的 Lua 字符串 */
static int
ltostring(lua_State *L) {
	if (lua_isnoneornil(L,1)) {
		return 0;
	}
	char * msg = lua_touserdata(L,1);
	int sz = luaL_checkinteger(L,2);
	lua_pushlstring(L,msg,sz);
	return 1;
}

/* [lua_api] 校验一个服务句柄是否是远程服务, 并得到其 harbor 值.
 * 参数: int[1] 服务句柄;
 * 返回: int[1] harbor 值; boolean[2] 是否为远程服务; */
static int
lharbor(lua_State *L) {
	struct skynet_context * context = lua_touserdata(L, lua_upvalueindex(1));
	uint32_t handle = (uint32_t)luaL_checkinteger(L,1);
	int harbor = 0;
	int remote = skynet_isremote(context, handle, &harbor);
	lua_pushinteger(L,harbor);
	lua_pushboolean(L, remote);

	return 2;
}

/* [lua_api] 将参数序列化成一个字符串. 参数类型必须是 nil, number, boolean, string, lightuserdata 和 table 中的一种, 否则将抛出错误.
 * 如果参数是表, 它嵌套的子表不能超过 MAX_DEPTH 层. 函数返回打包成功后的字符串. 当参数不符合要求时将抛出错误. */
static int
lpackstring(lua_State *L) {
	luaseri_pack(L);
	char * str = (char *)lua_touserdata(L, -2);
	int sz = lua_tointeger(L, -1);
	lua_pushlstring(L, str, sz);
	skynet_free(str);
	return 1;
}

/* [lua_api] 释放轻量用户数据的内存. 参数只能是 Lua 字符串或者轻量用户数据两种, 如果是轻量用户数据还必须在后边跟随数据的长度.
 * 其它类型的参数将抛出错误. 如果参数是 Lua 字符串其实是不释放内存的.
 *
 * 参数: string/lightuserdata[1] 待释放内存的参数; int[2] 如果是轻量用户数据, 此参数表示数据的长度;
 * 返回: 无返回值 */
static int
ltrash(lua_State *L) {
	int t = lua_type(L,1);
	switch (t) {
	case LUA_TSTRING: {
		break;
	}
	case LUA_TLIGHTUSERDATA: {
		void * msg = lua_touserdata(L,1);
		luaL_checkinteger(L,2);
		skynet_free(msg);
		break;
	}
	default:
		luaL_error(L, "skynet.trash invalid param %s", lua_typename(L,t));
	}

	return 0;
}

/* [lua_api] 获取当前时间, 单位是厘秒. 这个值可以看作是自系统启动以来经过的厘秒数. */
static int
lnow(lua_State *L) {
	uint64_t ti = skynet_now();
	lua_pushinteger(L, ti);
	return 1;
}

/* 将 C 函数注册为 Lua 函数, 所有函数则都会共享服务实例对象为上值. */
int
luaopen_skynet_core(lua_State *L) {
	luaL_checkversion(L);

	luaL_Reg l[] = {
		{ "send" , lsend },
		{ "genid", lgenid },
		{ "redirect", lredirect },
		{ "command" , lcommand },
		{ "intcommand", lintcommand },
		{ "error", lerror },
		{ "tostring", ltostring },
		{ "harbor", lharbor },
		{ "pack", luaseri_pack },
		{ "unpack", luaseri_unpack },
		{ "packstring", lpackstring },
		{ "trash" , ltrash },
		{ "callback", lcallback },
		{ "now", lnow },
		{ NULL, NULL },
	};

	luaL_newlibtable(L, l);

	lua_getfield(L, LUA_REGISTRYINDEX, "skynet_context");
	struct skynet_context *ctx = lua_touserdata(L,-1);
	if (ctx == NULL) {
		return luaL_error(L, "Init skynet context first");
	}

	luaL_setfuncs(L,l,1);

	return 1;
}
