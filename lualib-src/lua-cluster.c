#include <lua.h>
#include <lauxlib.h>
#include <string.h>
#include <assert.h>

#include "skynet.h"

/*
	uint32_t/string addr 
	uint32_t/session session
	lightuserdata msg
	uint32_t sz

	return 
		string request
		uint32_t next_session
 */

#define TEMP_LENGTH 0x8200
#define MULTI_PART 0x8000

/* 以小端形式向缓冲中填充一个无符号整形值.
 * 参数: buf 是待填充的缓冲, 缓冲的大小必须大于等于 4; n 是需要填充的无符号整型值;
 * 函数无返回值 */
static void
fill_uint32(uint8_t * buf, uint32_t n) {
	buf[0] = n & 0xff;
	buf[1] = (n >> 8) & 0xff;
	buf[2] = (n >> 16) & 0xff;
	buf[3] = (n >> 24) & 0xff;
}

/* 以大端形式向缓冲中填充长度 sz, 要求 sz 的值的范围必须在 0x10000(2 个字节)以内, 并且有足量的缓冲内存.
 * 参数: L 为 Lua 虚拟机栈, 在此函数中未使用; buf 是缓存起点地址; sz 是长度值;
 * 函数无返回值 */
static void
fill_header(lua_State *L, uint8_t *buf, int sz) {
	assert(sz < 0x10000);
	buf[0] = (sz >> 8) & 0xff;
	buf[1] = sz & 0xff;
}

/*
	The request package :
	size <= 0x8000 (32K) and address is id
		WORD sz+9
		BYTE 0
		DWORD addr
		DWORD session
		PADDING msg(sz)
	size > 0x8000 and address is id
		DWORD 13
		BYTE 1	; multi req	
		DWORD addr
		DWORD session
		DWORD sz

	size <= 0x8000 (32K) and address is string
		WORD sz+6+namelen
		BYTE 0x80
		BYTE namelen
		STRING name
		DWORD session
		PADDING msg(sz)
	size > 0x8000 and address is string
		DWORD 10 + namelen
		BYTE 0x81
		BYTE namelen
		STRING name
		DWORD session
		DWORD sz

	multi req
		WORD sz + 5
		BYTE 2/3 ; 2:multipart, 3:multipart end
		DWORD SESSION
		PADDING msgpart(sz)
 */
/* 当请求的服务地址是整数时对数据进行打包, 函数依据数据的大小(是否大于等于 0x8000)决定是否需要分包.
 * 如果不需要分包将对数据直接进行打包, 否则将只打包数据头, 而将具体每个分包的打包交由 packreq_multi 完成.
 *
 * 参数: L 为 Lua 虚拟机栈, 其 [1] 位置上为整形的服务地址, 并且将接收打包后的结果;
 *      session 是当前请求的回话号; msg 是请求体内容; sz 是请求体大小;
 *
 * 返回: 如果不需要分包将返回 0, 否则将返回分包数; */
static int
packreq_number(lua_State *L, int session, void * msg, uint32_t sz) {
	uint32_t addr = (uint32_t)lua_tointeger(L,1);
	uint8_t buf[TEMP_LENGTH];
	if (sz < MULTI_PART) {
		fill_header(L, buf, sz+9);
		buf[2] = 0;
		fill_uint32(buf+3, addr);
		fill_uint32(buf+7, (uint32_t)session);
		memcpy(buf+11,msg,sz);

		lua_pushlstring(L, (const char *)buf, sz+11);
		return 0;
	} else {
		int part = (sz - 1) / MULTI_PART + 1;
		fill_header(L, buf, 13);
		buf[2] = 1;
		fill_uint32(buf+3, addr);
		fill_uint32(buf+7, (uint32_t)session);
		fill_uint32(buf+11, sz);
		lua_pushlstring(L, (const char *)buf, 15);
		return part;
	}
}

/* 当请求的服务地址是字符串时对数据进行打包, 函数依据数据的大小(是否大于等于 0x8000)决定是否需要分包.
 * 如果不需要分包将对数据直接进行打包, 否则将只打包数据头, 而将具体每个分包的打包交由 packreq_multi 完成.
 * 注意: 名字的长度不能超过 255, 且不能没有名字.
 *
 * 参数: L 为 Lua 虚拟机栈, 其 [1] 位置上为字符串的服务地址, 并且将接收打包后的结果;
 *      session 是当前请求的回话号; msg 是请求体内容; sz 是请求体大小;
 *
 * 返回: 如果不需要分包将返回 0, 否则将返回分包数; */
static int
packreq_string(lua_State *L, int session, void * msg, uint32_t sz) {
	size_t namelen = 0;
	const char *name = lua_tolstring(L, 1, &namelen);
	if (name == NULL || namelen < 1 || namelen > 255) {
		skynet_free(msg);
		luaL_error(L, "name is too long %s", name);
	}

	uint8_t buf[TEMP_LENGTH];
	if (sz < MULTI_PART) {
		fill_header(L, buf, sz+6+namelen);
		buf[2] = 0x80;
		buf[3] = (uint8_t)namelen;
		memcpy(buf+4, name, namelen);
		fill_uint32(buf+4+namelen, (uint32_t)session);
		memcpy(buf+8+namelen,msg,sz);

		lua_pushlstring(L, (const char *)buf, sz+8+namelen);
		return 0;
	} else {
		int part = (sz - 1) / MULTI_PART + 1;
		fill_header(L, buf, 10+namelen);
		buf[2] = 0x81;
		buf[3] = (uint8_t)namelen;
		memcpy(buf+4, name, namelen);
		fill_uint32(buf+4+namelen, (uint32_t)session);
		fill_uint32(buf+8+namelen, sz);

		lua_pushlstring(L, (const char *)buf, 12+namelen);
		return part;
	}
}

/* 打包分包, 分包数据将会压栈到 Lua 虚拟机栈的栈顶的表中, 按照顺序组成序列.
 * 参数: L 为接收打包分包数据的 Lua 虚拟机栈; session 是当前请求的回话号; msg 当前需要打包的请求体; sz 是请求体的大小;
 * 函数无返回值 */
static void
packreq_multi(lua_State *L, int session, void * msg, uint32_t sz) {
	uint8_t buf[TEMP_LENGTH];
	int part = (sz - 1) / MULTI_PART + 1;
	int i;
	char *ptr = msg;
	for (i=0;i<part;i++) {
		uint32_t s;
		if (sz > MULTI_PART) {
			s = MULTI_PART;
			buf[2] = 2;
		} else {
			s = sz;
			buf[2] = 3;	// the last multi part
		}
		fill_header(L, buf, s+5);
		fill_uint32(buf+3, (uint32_t)session);
		memcpy(buf+7, ptr, s);
		lua_pushlstring(L, (const char *)buf, s+7);
		lua_rawseti(L, -2, i+1);
		sz -= s;
		ptr += s;
	}
}

/* [lua_api] 打包一个请求体, 函数能够依据请求体的大小进行相应的分包. 请求的服务地址可以是整形也可以是字符串.
 *
 * 参数: string or integer [1] 请求服务的地址; integer [2] 为本次请求的回话号; light userdata [3] 为请求体;
 *      integer [4] 是请求体的大小;
 *
 * 返回: string [1] 不论是否分包都将返回的第一个消息; integer [2] 下一次请求时使用的新的会话号;
 *      table or nil [3] 仅在分包的情况下返回的后续分包数据; */
static int
lpackrequest(lua_State *L) {
	void *msg = lua_touserdata(L,3);
	if (msg == NULL) {
		return luaL_error(L, "Invalid request message");
	}
	uint32_t sz = (uint32_t)luaL_checkinteger(L,4);
	int session = luaL_checkinteger(L,2);
	if (session <= 0) {
		skynet_free(msg);
		return luaL_error(L, "Invalid request session %d", session);
	}
	int addr_type = lua_type(L,1);
	int multipak;
	if (addr_type == LUA_TNUMBER) {
		multipak = packreq_number(L, session, msg, sz);
	} else {
		multipak = packreq_string(L, session, msg, sz);
	}
	int current_session = session;
	if (++session < 0) {
		session = 1;
	}
	lua_pushinteger(L, session);
	if (multipak) {
		lua_createtable(L, multipak, 0);
		packreq_multi(L, current_session, msg, sz);
		skynet_free(msg);
		return 3;
	} else {
		skynet_free(msg);
		return 2;
	}
}

/*
	string packed message
	return 	
		uint32_t or string addr
		int session
		string msg
		boolean padding
 */
/* 以小端方式从 buf 中解包一个无符号整数. 要求 buf 中至少有 4 个字节的内容.
 * 参数: buf 是缓存;
 * 返回: 解包出来的无符号整数; */
static inline uint32_t
unpack_uint32(const uint8_t * buf) {
	return buf[0] | buf[1]<<8 | buf[2]<<16 | buf[3]<<24;
}

/* 解包未分包且服务地址是整数的请求体. 函数将解包得到的请求的地址、会话号以及请求体数据压栈到 Lua 虚拟机栈上.
 * 参数: L 是 Lua 虚拟机栈用于接收解包得到的数据; buf 是由网络传递过来的缓冲数据; sz 是数据大小;
 * 返回: int [1] 是请求服务的地址; int [2] 是会话号; string [3] 是请求体数据; */
static int
unpackreq_number(lua_State *L, const uint8_t * buf, int sz) {
	if (sz < 9) {
		return luaL_error(L, "Invalid cluster message (size=%d)", sz);
	}
	uint32_t address = unpack_uint32(buf+1);
	uint32_t session = unpack_uint32(buf+5);
	lua_pushinteger(L, address);
	lua_pushinteger(L, session);
	lua_pushlstring(L, (const char *)buf+9, sz-9);

	return 3;
}

/* 解包服务地址是整数的多分包请求体数据的第一个消息, 消息中并不包含实际的数据, 而是告知 Lua 层后边还有更多的数据.
 * 参数: L 是 Lua 虚拟机栈用于接收解包得到的数据; buf 是由网络传递过来的缓冲数据; sz 是数据大小;
 * 返回: int [1] 是请求服务的地址; int [2] 是会话号; int [3] 是数据的大小; boolean [4] 一定是 true 表明后续还有数据; */
static int
unpackmreq_number(lua_State *L, const uint8_t * buf, int sz) {
	if (sz != 13) {
		return luaL_error(L, "Invalid cluster message size %d (multi req must be 13)", sz);
	}
	uint32_t address = unpack_uint32(buf+1);
	uint32_t session = unpack_uint32(buf+5);
	uint32_t size = unpack_uint32(buf+9);
	lua_pushinteger(L, address);
	lua_pushinteger(L, session);
	lua_pushinteger(L, size);
	lua_pushboolean(L, 1);	// padding multi part

	return 4;
}

/* 解包后续的分包数据. 同时告知 Lua 层后边是否还有后续数据.
 * 参数: L 是 Lua 虚拟机栈用于接收解包得到的数据; buf 是由网络传递过来的缓冲数据; sz 是数据大小;
 *
 * 返回: boolean [1] 一定是 false 表明没有地址; integer [2] 这个请求对应的会话号; string [3] 是请求体内容;
 *      boolean [4] 表明后续是否还有数据; */
static int
unpackmreq_part(lua_State *L, const uint8_t * buf, int sz) {
	if (sz < 5) {
		return luaL_error(L, "Invalid cluster multi part message");
	}
	int padding = (buf[0] == 2);
	uint32_t session = unpack_uint32(buf+1);
	lua_pushboolean(L, 0);	// no address
	lua_pushinteger(L, session);
	lua_pushlstring(L, (const char *)buf+5, sz-5);
	lua_pushboolean(L, padding);

	return 4;
}

/* 解包未分包且服务地址是字符串的请求体. 函数将解包得到的请求的地址、会话号以及请求体数据压栈到 Lua 虚拟机栈上.
 * 参数: L 是 Lua 虚拟机栈用于接收解包得到的数据; buf 是由网络传递过来的缓冲数据; sz 是数据大小;
 * 返回: string [1] 是请求服务的地址; int [2] 是会话号; string [3] 是请求体数据; */
static int
unpackreq_string(lua_State *L, const uint8_t * buf, int sz) {
	if (sz < 2) {
		return luaL_error(L, "Invalid cluster message (size=%d)", sz);
	}
	size_t namesz = buf[1];
	if (sz < namesz + 6) {
		return luaL_error(L, "Invalid cluster message (size=%d)", sz);
	}
	lua_pushlstring(L, (const char *)buf+2, namesz);
	uint32_t session = unpack_uint32(buf + namesz + 2);
	lua_pushinteger(L, (uint32_t)session);
	lua_pushlstring(L, (const char *)buf+2+namesz+4, sz - namesz - 6);

	return 3;
}

/* 解包服务地址是字符串的多分包请求体数据的第一个消息, 消息中并不包含实际的数据, 而是告知 Lua 层后边还有更多的数据.
 * 参数: L 是 Lua 虚拟机栈用于接收解包得到的数据; buf 是由网络传递过来的缓冲数据; sz 是数据大小;
 * 返回: string [1] 是请求服务的地址; int [2] 是会话号; int [3] 是数据的大小; boolean [4] 一定是 true 表明后续还有数据; */
static int
unpackmreq_string(lua_State *L, const uint8_t * buf, int sz) {
	if (sz < 2) {
		return luaL_error(L, "Invalid cluster message (size=%d)", sz);
	}
	size_t namesz = buf[1];
	if (sz < namesz + 10) {
		return luaL_error(L, "Invalid cluster message (size=%d)", sz);
	}
	lua_pushlstring(L, (const char *)buf+2, namesz);
	uint32_t session = unpack_uint32(buf + namesz + 2);
	uint32_t size = unpack_uint32(buf + namesz + 6);
	lua_pushinteger(L, session);
	lua_pushinteger(L, size);
	lua_pushboolean(L, 1);	// padding multipart

	return 4;
}

/* [lua_api] 对由网络传递过来的请求数据进行解包. 函数依据数据类型的不同进行不同方式的解包.
 * 参数: light userdata [1] 请求体数据;
 * 返回: int or string or false [1] 当为 int 时表明是整形地址, string 时表明是字符串类型地址, boolean false 表明当前是一个后续分包;
 *       int [2] 会话号; int or string [3] 若为 int 表明后续数据的大小, string 时是数据内容;
 *       boolean [4] 表明后续是否还有数据;  */
static int
lunpackrequest(lua_State *L) {
	size_t ssz;
	const char *msg = luaL_checklstring(L,1,&ssz);
	int sz = (int)ssz;
	switch (msg[0]) {
	case 0:
		return unpackreq_number(L, (const uint8_t *)msg, sz);
	case 1:
		return unpackmreq_number(L, (const uint8_t *)msg, sz);
	case 2:
	case 3:
		return unpackmreq_part(L, (const uint8_t *)msg, sz);
	case '\x80':
		return unpackreq_string(L, (const uint8_t *)msg, sz);
	case '\x81':
		return unpackmreq_string(L, (const uint8_t *)msg, sz);
	default:
		return luaL_error(L, "Invalid req package type %d", msg[0]);
	}
}

/*
	DWORD session
	BYTE type
		0: error
		1: ok
		2: multi begin
		3: multi part
		4: multi end
	PADDING msg
		type = 0, error msg
		type = 1, msg
		type = 2, DWORD size
		type = 3/4, msg
 */
/*
	int session
	boolean ok
	lightuserdata msg
	int sz
	return string response
 */
/* [lua_api] 将响应数据进行打包, 打包函数会依据消息的长度进行合适的分包. 如果数据长度超过了 0x8000 时将会数据分成多个部分.
 * 第一部分标示数据的长度, 其余部分按照 0x8000 长度分包. 所有数据将放到一个序列中返回给 Lua 层. 否则将直接打包成字符串返回.
 * 对于出错消息将始终返回一个字符串. 打包的格式如上描述.
 *
 * 参数: integer [1] 为本次回复对应的回话号; boolean [2] 说明本次请求是否处理正确; string [3] 为数据内容,
 *       或者 lightuserdata [3] 数据内容 int [4] 数据长度;
 *
 * 返回: string [1] 当消息长度在一个包内, 或者是错误消息时将会完全打包成一个字符串, 或者 table [1] 当数据需要分包时, 将数据打包到序列中去. */
static int
lpackresponse(lua_State *L) {
	uint32_t session = (uint32_t)luaL_checkinteger(L,1);
	// clusterd.lua:command.socket call lpackresponse,
	// and the msg/sz is return by skynet.rawcall , so don't free(msg)
	int ok = lua_toboolean(L,2);
	void * msg;
	size_t sz;
	
	if (lua_type(L,3) == LUA_TSTRING) {
		msg = (void *)lua_tolstring(L, 3, &sz);
	} else {
		msg = lua_touserdata(L,3);
		sz = (size_t)luaL_checkinteger(L, 4);
	}

	if (!ok) {
		if (sz > MULTI_PART) {
			// truncate the error msg if too long
			sz = MULTI_PART;
		}
	} else {
		if (sz > MULTI_PART) {
			// return 
			int part = (sz - 1) / MULTI_PART + 1;
			lua_createtable(L, part+1, 0);
			uint8_t buf[TEMP_LENGTH];

			// multi part begin
			fill_header(L, buf, 9);
			fill_uint32(buf+2, session);
			buf[6] = 2;
			fill_uint32(buf+7, (uint32_t)sz);
			lua_pushlstring(L, (const char *)buf, 11);
			lua_rawseti(L, -2, 1);

			char * ptr = msg;
			int i;
			for (i=0;i<part;i++) {
				int s;
				if (sz > MULTI_PART) {
					s = MULTI_PART;
					buf[6] = 3;
				} else {
					s = sz;
					buf[6] = 4;
				}
				fill_header(L, buf, s+5);
				fill_uint32(buf+2, session);
				memcpy(buf+7,ptr,s);
				lua_pushlstring(L, (const char *)buf, s+7);
				lua_rawseti(L, -2, i+2);
				sz -= s;
				ptr += s;
			}
			return 1;
		}
	}

	uint8_t buf[TEMP_LENGTH];
	fill_header(L, buf, sz+5);
	fill_uint32(buf+2, session);
	buf[6] = ok;
	memcpy(buf+7,msg,sz);

	lua_pushlstring(L, (const char *)buf, sz+7);

	return 1;
}

/*
	string packed response
	return integer session
		boolean ok
		string msg
		boolean padding
 */
/* [lua_api] 解包一个对端回复过来的数据. 函数依据数据的第 5 个字节标明的 type 对数据进行不同方式的解包.
 *
 * 参数: string [1] 为需要解包的数据, 对应于 lpackresponse 中单个分包;
 *
 * 返回: integer [1] 本次回复对应的会话号; boolean [2] 是否处理成功; string or integer [3] 表示数据内容, 或者后续数据的大小;
 *       boolean [4] 表示是否还有后续数据; */
static int
lunpackresponse(lua_State *L) {
	size_t sz;
	const char * buf = luaL_checklstring(L, 1, &sz);
	if (sz < 5) {
		return 0;
	}
	uint32_t session = unpack_uint32((const uint8_t *)buf);
	lua_pushinteger(L, (lua_Integer)session);
	switch(buf[4]) {
	case 0:	// error
		lua_pushboolean(L, 0);
		lua_pushlstring(L, buf+5, sz-5);
		return 3;
	case 1:	// ok
	case 4:	// multi end
		lua_pushboolean(L, 1);
		lua_pushlstring(L, buf+5, sz-5);
		return 3;
	case 2:	// multi begin
		if (sz != 9) {
			return 0;
		}
		sz = unpack_uint32((const uint8_t *)buf+5);
		lua_pushboolean(L, 1);
		lua_pushinteger(L, sz);
		lua_pushboolean(L, 1);
		return 4;
	case 3:	// multi part
		lua_pushboolean(L, 1);
		lua_pushlstring(L, buf+5, sz-5);
		lua_pushboolean(L, 1);
		return 4;
	default:
		return 0;
	}
}

/* [lua_api] 将序列中的数据拼接起来. 要求序列的第一个元素一定是后续数据的总长度. 待拼接的序列通常是集群对端发送过来的多分包响应.
 * 参数: table [1] 为待拼接的数据;
 * 返回: lightuserdata [1] 是拼接起来的数据; integer [2] 是数据的大小; */
static int
lconcat(lua_State *L) {
	if (!lua_istable(L,1))
		return 0;
	if (lua_geti(L,1,1) != LUA_TNUMBER)
		return 0;
	int sz = lua_tointeger(L,-1);
	lua_pop(L,1);
	char * buff = skynet_malloc(sz);
	int idx = 2;
	int offset = 0;
	while(lua_geti(L,1,idx) == LUA_TSTRING) {
		size_t s;
		const char * str = lua_tolstring(L, -1, &s);
		if (s+offset > sz) {
			skynet_free(buff);
			return 0;
		}
		memcpy(buff+offset, str, s);
		lua_pop(L,1);
		offset += s;
		++idx;
	}
	if (offset != sz) {
		skynet_free(buff);
		return 0;
	}
	// buff/sz will send to other service, See clusterd.lua
	lua_pushlightuserdata(L, buff);
	lua_pushinteger(L, sz);
	return 2;
}

int
luaopen_cluster_core(lua_State *L) {
	luaL_Reg l[] = {
		{ "packrequest", lpackrequest },
		{ "unpackrequest", lunpackrequest },
		{ "packresponse", lpackresponse },
		{ "unpackresponse", lunpackresponse },
		{ "concat", lconcat },
		{ NULL, NULL },
	};
	luaL_checkversion(L);
	luaL_newlib(L,l);

	return 1;
}
