#include "skynet.h"
#include "skynet_env.h"
#include "spinlock.h"

#include <lua.h>
#include <lauxlib.h>

#include <stdlib.h>
#include <assert.h>

struct skynet_env {
	struct spinlock lock;
	lua_State *L;
};

static struct skynet_env *E = NULL;

/* 从 skynet 中获取名为 key 所表示的环境变量.
 * 若此变量存在则返回相应的值, 若不存在则返回 NULL.
 * 此函数是线程安全的. */
const char * 
skynet_getenv(const char *key) {
	SPIN_LOCK(E)

	lua_State *L = E->L;
	
	lua_getglobal(L, key);
	const char * result = lua_tostring(L, -1);
	lua_pop(L, 1);

	SPIN_UNLOCK(E)

	return result;
}

/* 给 skynet 设置一个环境变量, 要求之前不存在此环境变量.
 * 参数 key 是环境变量名, value 为关联的值.
 * 此函数是线程安全的. */
void 
skynet_setenv(const char *key, const char *value) {
	SPIN_LOCK(E)
	
	lua_State *L = E->L;
	lua_getglobal(L, key);
	assert(lua_isnil(L, -1));
	lua_pop(L,1);
	lua_pushstring(L,value);
	lua_setglobal(L,key);

	SPIN_UNLOCK(E)
}

/* 初始化环境变量管理器. */
void
skynet_env_init() {
	E = skynet_malloc(sizeof(*E));
	SPIN_INIT(E)
	E->L = luaL_newstate();
}
