#include <stdio.h>
#include <lua.h>
#include <lauxlib.h>

#include <time.h>

#if defined(__APPLE__)
#include <mach/task.h>
#include <mach/mach.h>
#endif

#define NANOSEC 1000000000
#define MICROSEC 1000000

// #define DEBUG_LOG

/* 获取当前线程自服务启动以来的统计运行时间作为当前时间.
 * 依据系统的不同而使用不同的函数获取, 最终结果以双精度浮点数的形式返回.
 * 返回的运行时间会被对齐到 0xffff 返回内, 这对统计两个时间点之间的时间流逝没有影响.
 * 此函数用于作为取得运行时间点, 并统计两个时间点之间的差值, 从而得到统计任务的运行时间. */
static double
get_time() {
#if  !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ti);

	int sec = ti.tv_sec & 0xffff;
	int nsec = ti.tv_nsec;

	return (double)sec + (double)nsec / NANOSEC;	
#else
	/* 在 OSX 下使用 GNU mach task information 函数获取当前线程的统计运行时 */
	struct task_thread_times_info aTaskInfo;
	mach_msg_type_number_t aTaskInfoCount = TASK_THREAD_TIMES_INFO_COUNT;
	if (KERN_SUCCESS != task_info(mach_task_self(), TASK_THREAD_TIMES_INFO, (task_info_t )&aTaskInfo, &aTaskInfoCount)) {
		return 0;
	}

	int sec = aTaskInfo.user_time.seconds & 0xffff;
	int msec = aTaskInfo.user_time.microseconds;

	return (double)sec + (double)msec / MICROSEC;
#endif
}

/* 计算当前时间和开始时间 start 之间的差值. 这个差值被认为不会超过 0xffff 秒.
 * 此差值在 skynet 中被用于计算一个任务执行的时间. */
static inline double 
diff_time(double start) {
	double now = get_time();
	if (now < start) {
		return now + 0x10000 - start;
	} else {
		return now - start;
	}
}

/* [lua_api] 开启一个 skynet 服务运行时间统计, 函数传入一个 Lua 线程作为唯一参数, 如果未提供此参数,
 * 将以当前 Lua 线程作为参数. 函数将记录当前线程运行的时间作为统计的起点, 并在每次让出时计算一个运行时间,
 * 和最终停止时计算并返回这个运行时间统计值. 此函数对于任何一个 Lua 线程只能调用一次, 如果调用多次将抛出错误.
 *
 * 正确的使用方法是在 skynet 服务的主线程中调用 lstart 函数, 而可以在主线程中的任何子协程中调用 lstop 函数.
 *
 * 参数: thread[1] 是需要统计运行时的 Lua 线程, 若提供应当为服务的主线程, 如果未提供则以当前 Lua 线程作为统计线程;
 * 函数无返回值 */
static int
lstart(lua_State *L) {
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	/* 第二个上值用于保存总的运行时间, 第一个上值用于保存上一次运行时间点. */
	lua_rawget(L, lua_upvalueindex(2));
	if (!lua_isnil(L, -1)) {
		return luaL_error(L, "Thread %p start profile more than once", lua_topointer(L, 1));
	}
	lua_pushthread(L);
	lua_pushnumber(L, 0);
	lua_rawset(L, lua_upvalueindex(2));

	lua_pushthread(L);
	double ti = get_time();
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] start\n", L);
#endif
	lua_pushnumber(L, ti);
	lua_rawset(L, lua_upvalueindex(1));

	return 0;
}

/* [lua_api] 关闭 skynet 服务的运行时间统计, 并返回运行时间. 函数传入一个 Lua 线程作为唯一参数,
 * 如果未提供此参数, 将以当前 Lua 线程为参数. 当最终计算完整个运行时间之后, 将从上值表中移除当前 Lua 线程.
 * 此函数不允许多次调用, 否则将抛出错误.
 *
 * 函数结束的是当前线程的运行时间统计, 可以在 skynet 主线程和子线程中调用.
 *
 * 参数: thread[1] 是需要结束统计运行时的 Lua 线程, 若提供应当为服务的主线程, 如果未提供则以当前 Lua 线程作为统计线程;
 * 函数无返回值 */
static int
lstop(lua_State *L) {
	if (lua_type(L,1) == LUA_TTHREAD) {
		lua_settop(L,1);
	} else {
		lua_pushthread(L);
	}
	lua_rawget(L, lua_upvalueindex(1));
	if (lua_type(L, -1) != LUA_TNUMBER) {
		return luaL_error(L, "Call profile.start() before profile.stop()");
	}
	double ti = diff_time(lua_tonumber(L, -1));
	lua_pushthread(L);
	lua_rawget(L, lua_upvalueindex(2));
	double total_time = lua_tonumber(L, -1);

	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(1));

	lua_pushthread(L);
	lua_pushnil(L);
	lua_rawset(L, lua_upvalueindex(2));

	total_time += ti;
	lua_pushnumber(L, total_time);
#ifdef DEBUG_LOG
	fprintf(stderr, "PROFILE [%p] stop (%lf / %lf)\n", L, ti, total_time);
#endif

	return 1;
}

/* 延续一个 Lua 线程, 如果是需要统计运行的, 就将当前时间保存起来用于计算此次运行时间. 栈顶的线程是 skynet 的服务主线程,
 * 若检查发现主线程开启了运行时间统计, 则在马上要延续的线程中写入当前时间. */
static int
timing_resume(lua_State *L) {
#ifdef DEBUG_LOG
	lua_State *from = lua_tothread(L, -1);
#endif
	lua_rawget(L, lua_upvalueindex(2));
	if (lua_isnil(L, -1)) {		// check total time
		lua_pop(L,1);
	} else {
		lua_pop(L,1);
		lua_pushvalue(L,1);
		double ti = get_time();
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] resume\n", from);
#endif
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(1));	// set start time
	}

	lua_CFunction co_resume = lua_tocfunction(L, lua_upvalueindex(3));

	return co_resume(L);
}

/* [lua_api] 延续一个 Lua 线程, 待延续的线程作为第一个参数传入函数. 如果线程处于 lstart 的调用范围,
 * 还将记录当前时间, 并在下一次让出时统计运行时间之和. 函数的调用方式和返回值与 coroutine.resume 一样.
 * 这个函数最主要用于延续一个 Lua 服务线程. */
static int
lresume(lua_State *L) {
	lua_pushvalue(L,1);
	
	return timing_resume(L);
}

/* [lua_api] 延续一个 Lua 线程, 待延续的线程作为第一个参数传入函数, 服务主线程作为第二个参数传入.
 * 后续的参数与 coroutine.resume 除第一个参数外一致. 这个函数最主要用于从指令调用返回. */
static int
lresume_co(lua_State *L) {
	luaL_checktype(L, 2, LUA_TTHREAD);
	/* 在索引为 2 的位置向下轮转一个位置, 将导致 2 位置的值在栈顶, 其上面的值向下移动一位 */
	lua_rotate(L, 2, -1);

	return timing_resume(L);
}

/* 从 Lua 线程中让出, 此时服务主线程已经在栈顶. 若需要统计运行时间, 则将当前线程运行的时间加上上一次主线程的运行时间.
 * 正确的用法是这个函数只会在子线程给框架发送指令 (CALL 、 SLEEP 、 RESPONSE) 时才会调用此函数. */
static int
timing_yield(lua_State *L) {
#ifdef DEBUG_LOG
	lua_State *from = lua_tothread(L, -1);
#endif
	lua_rawget(L, lua_upvalueindex(2));	// check total time
	if (lua_isnil(L, -1)) {
		lua_pop(L,1);
	} else {
		double ti = lua_tonumber(L, -1);
		lua_pop(L,1);

		lua_pushthread(L);
		lua_rawget(L, lua_upvalueindex(1));
		double starttime = lua_tonumber(L, -1);
		lua_pop(L,1);

		double diff = diff_time(starttime);
		ti += diff;
#ifdef DEBUG_LOG
		fprintf(stderr, "PROFILE [%p] yield (%lf/%lf)\n", from, diff, ti);
#endif

		lua_pushthread(L);
		lua_pushnumber(L, ti);
		lua_rawset(L, lua_upvalueindex(2));
	}

	lua_CFunction co_yield = lua_tocfunction(L, lua_upvalueindex(3));

	return co_yield(L);
}

/* [lua_api] 让出一个线程, 其调用方式与 coroutine.yield 一致. 这个函数最主要的用法是让出 skynet 服务,
 * 如各种调用指令. */
static int
lyield(lua_State *L) {
	lua_pushthread(L);

	return timing_yield(L);
}

/* [lua_api] 从 skynet 服务的子协程中让出. 第一个参数一定是服务的主线程. 当让出时, 如果需要统计运行时间将会统计
 * 当前线程运行的时间加上上一次主线程的运行时间. 调用的时机是子线程给框架发送指令 (CALL 、 SLEEP 、 RESPONSE) 时. */
static int
lyield_co(lua_State *L) {
	luaL_checktype(L, 1, LUA_TTHREAD);
	lua_rotate(L, 1, -1);
	
	return timing_yield(L);
}

/* 注册统计服务消息处理时间的函数到 Lua 中, 并设置相关的上值用于记录统计值或者是 coroutine.resume 和
 * coroutine.yield 函数. */
int
luaopen_profile(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "start", lstart },
		{ "stop", lstop },
		{ "resume", lresume },
		{ "yield", lyield },
		{ "resume_co", lresume_co },
		{ "yield_co", lyield_co },
		{ NULL, NULL },
	};
	luaL_newlibtable(L,l);
	lua_newtable(L);	// table thread->start time
	lua_newtable(L);	// table thread->total time

	lua_newtable(L);	// weak table
	lua_pushliteral(L, "kv");
	lua_setfield(L, -2, "__mode");

	lua_pushvalue(L, -1);
	lua_setmetatable(L, -3);
	lua_setmetatable(L, -3);

	lua_pushnil(L);	// cfunction (coroutine.resume or coroutine.yield)
	luaL_setfuncs(L,l,3);

	int libtable = lua_gettop(L);

	lua_getglobal(L, "coroutine");
	lua_getfield(L, -1, "resume");

	lua_CFunction co_resume = lua_tocfunction(L, -1);
	if (co_resume == NULL)
		return luaL_error(L, "Can't get coroutine.resume");
	lua_pop(L,1);

	lua_getfield(L, libtable, "resume");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, libtable, "resume_co");
	lua_pushcfunction(L, co_resume);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, -1, "yield");

	lua_CFunction co_yield = lua_tocfunction(L, -1);
	if (co_yield == NULL)
		return luaL_error(L, "Can't get coroutine.yield");
	lua_pop(L,1);

	lua_getfield(L, libtable, "yield");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_getfield(L, libtable, "yield_co");
	lua_pushcfunction(L, co_yield);
	lua_setupvalue(L, -2, 3);
	lua_pop(L,1);

	lua_settop(L, libtable);

	return 1;
}
