#include "skynet_malloc.h"

#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <assert.h>

#include <lua.h>
#include <lauxlib.h>

#include <sys/socket.h>
#include <arpa/inet.h>

#include "skynet_socket.h"

#define BACKLOG 32
// 2 ** 12 == 4096
#define LARGE_PAGE_NODE 12     /* 一次性分配 buffer_node 的最大数目为 2**LARGE_PAGE_NODE */

/* 套接字接收到的套接字消息的缓存节点 */
struct buffer_node {
	char * msg;                  /* 套接字消息内容 */
	int sz;                      /* 套接字消息大小 */
	struct buffer_node *next;    /* 下一个缓存节点, 先接收到的消息在前面 */
};

/* 所有的套接字信息的缓存列表, 按照先到来的消息在前面, 后到来的消息在后面排列 */
struct socket_buffer {
	int size;                    /* 所有消息的总大小, 随着到来而增加, 随着被处理而减少 */
	int offset;                  /* 读取当前缓存节点的消息的起点, 因为读取操作可能不是一个完整的节点 */
	struct buffer_node *head;    /* 套接字缓存的头结点, 读取操作将读取此节点 */
	struct buffer_node *tail;    /* 套接字缓存的尾节点, 写入操作将写入到此节点的后边 */
};

/* [lua_api] 释放用于装载套接字消息的缓存节点数组中每个节点装载的套接字消息内存.
 * 参数: userdata [1] 是缓存节点数组.
 * 函数无返回值 */
static int
lfreepool(lua_State *L) {
	struct buffer_node * pool = lua_touserdata(L, 1);
	int sz = lua_rawlen(L,1) / sizeof(*pool);
	int i;
	for (i=0;i<sz;i++) {
		struct buffer_node *node = &pool[i];
		if (node->msg) {
			skynet_free(node->msg);
			node->msg = NULL;
		}
	}
	return 0;
}

/* 在 lua 虚拟机中生成一个完全用户数据作为新的缓存节点数组用于装载套接字消息.
 * 数组生成好之后将被链接起来并设置其 __gc 函数用以在回收内存时同时回收套接字消息的内存.
 * 函数返回时此缓存节点数组在虚拟机的栈顶. */
static int
lnewpool(lua_State *L, int sz) {
	struct buffer_node * pool = lua_newuserdata(L, sizeof(struct buffer_node) * sz);
	int i;
	for (i=0;i<sz;i++) {
		pool[i].msg = NULL;
		pool[i].sz = 0;
		pool[i].next = &pool[i+1];
	}
	pool[sz-1].next = NULL;
	if (luaL_newmetatable(L, "buffer_pool")) {
		lua_pushcfunction(L, lfreepool);
		lua_setfield(L, -2, "__gc");
	}
	lua_setmetatable(L, -2);
	return 1;
}

/* [lua_api] 在 lua 虚拟机中生成一个完全用户数据用作接收套接字信息的缓存列表.
 * 返回: userdata [1] 缓存列表; */
static int
lnewbuffer(lua_State *L) {
	struct socket_buffer * sb = lua_newuserdata(L, sizeof(*sb));	
	sb->size = 0;
	sb->offset = 0;
	sb->head = NULL;
	sb->tail = NULL;
	
	return 1;
}

/*
	userdata send_buffer
	table pool
	lightuserdata msg
	int size

	return size

	Comment: The table pool record all the buffers chunk, 
	and the first index [1] is a lightuserdata : free_node. We can always use this pointer for struct buffer_node .
	The following ([2] ...)  userdatas in table pool is the buffer chunk (for struct buffer_node), 
	we never free them until the VM closed. The size of first chunk ([2]) is 16 struct buffer_node,
	and the second size is 32 ... The largest size of chunk is LARGE_PAGE_NODE (4096)

	lpushbbuffer will get a free struct buffer_node from table pool, and then put the msg/size in it.
	lpopbuffer return the struct buffer_node back to table pool (By calling return_free_node).
 */
 /* [lua_api] 将一个套接字消息插入到缓存列表中去. 函数能够在缓存节点池中没有空闲节点时分配新的节点. 缓存节点池是一个 lua 表.
  * 分配的策略是当第一次分配时为 16 个缓存节点, 作为表的索引为 [2] 的元素, 再次分配则为 32 个缓存节点在 [3] 处,
  * 直到最大的 2**LARGE_PAGE_NODE(4096) 个缓存节点. 表的 [1] 处的元素保存了池中空闲的缓存节点链表, 当插入缓存列表中
  * 就空闲链表中取得, 当从缓存列表中使用完毕之后会在插入到空闲链表的头部. 整个缓存池会保存到虚拟机关闭.
  *
  * 参数: userdata [1] 为缓存列表; table [2] 为缓存节点池; lightuserdata [3] 为消息内容; int [4] 是消息内容大小;
  * 返回: int [1] 为整个缓存列表在插入消息之后的所有消息内容大小之和; */
static int
lpushbuffer(lua_State *L) {
	struct socket_buffer *sb = lua_touserdata(L,1);
	if (sb == NULL) {
		return luaL_error(L, "need buffer object at param 1");
	}
	char * msg = lua_touserdata(L,3);
	if (msg == NULL) {
		return luaL_error(L, "need message block at param 3");
	}
	int pool_index = 2;
	luaL_checktype(L,pool_index,LUA_TTABLE);
	int sz = luaL_checkinteger(L,4);
	lua_rawgeti(L,pool_index,1);
	struct buffer_node * free_node = lua_touserdata(L,-1);	// sb poolt msg size free_node
	lua_pop(L,1);
	if (free_node == NULL) {
		int tsz = lua_rawlen(L,pool_index);
		if (tsz == 0)
			tsz++;
		int size = 8;
		if (tsz <= LARGE_PAGE_NODE-3) {
			size <<= tsz;
		} else {
			size <<= LARGE_PAGE_NODE-3;
		}
		lnewpool(L, size);
		free_node = lua_touserdata(L,-1);
		lua_rawseti(L, pool_index, tsz+1);
	}
	lua_pushlightuserdata(L, free_node->next);
	lua_rawseti(L, pool_index, 1);	// sb poolt msg size
	free_node->msg = msg;
	free_node->sz = sz;
	free_node->next = NULL;

	if (sb->head == NULL) {
		assert(sb->tail == NULL);
		sb->head = sb->tail = free_node;
	} else {
		sb->tail->next = free_node;
		sb->tail = free_node;
	}
	sb->size += sz;

	lua_pushinteger(L, sb->size);

	return 1;
}

/* 回收缓存列表 sb 的头结点到缓存节点池中, 回收的同时将释放其消息体的内存. 并且会导致缓存列表的下次读取起点 offset 变为 0 .
 * 参数: L 为 lua 虚拟机, 其 pool 索引处为缓存节点池; pool 是缓存节点池所在的索引, 它是一个 lua 表; sb 是缓存列表;
 * 函数将保证结束时虚拟机上的值的位置不会改变 */
static void
return_free_node(lua_State *L, int pool, struct socket_buffer *sb) {
	struct buffer_node *free_node = sb->head;
	sb->offset = 0;
	sb->head = free_node->next;
	if (sb->head == NULL) {
		sb->tail = NULL;
	}
	lua_rawgeti(L,pool,1);
	free_node->next = lua_touserdata(L,-1);
	lua_pop(L,1);
	skynet_free(free_node->msg);
	free_node->msg = NULL;

	free_node->sz = 0;
	lua_pushlightuserdata(L, free_node);
	lua_rawseti(L, pool, 1);
}

/* 从缓存列表 sb 中读取长度为 sz 的字符串, 并将末尾的 skip 长度数据忽略掉, 而把前面 sz-skip 长度的数据放入
 * 到虚拟机 L 的栈顶. 读取完的缓存节点将被再此归还到缓存节点池中.
 *
 * 参数: L 为 lua 虚拟机, 其 [2] 处的元素是缓存节点池 pool; sb 是缓存列表; sz 是读取的大小, 其末尾的 skip 长度将被忽略;
 * 返回: L 的栈顶为读取到的字符串 */
static void
pop_lstring(lua_State *L, struct socket_buffer *sb, int sz, int skip) {
	struct buffer_node * current = sb->head;
	if (sz < current->sz - sb->offset) {
		lua_pushlstring(L, current->msg + sb->offset, sz-skip);
		sb->offset+=sz;
		return;
	}
	if (sz == current->sz - sb->offset) {
		lua_pushlstring(L, current->msg + sb->offset, sz-skip);
		return_free_node(L,2,sb);
		return;
	}

	/* 一次读取一个缓存节点中消息内容并将它们放到 Lua 虚拟机的字符串缓存中去, 直到最后才在栈顶留下一个字符串,
	 * 此处需要注意 skip 可能跨越多个缓存节点. */
	luaL_Buffer b;
	luaL_buffinit(L, &b);
	for (;;) {
		int bytes = current->sz - sb->offset;
		if (bytes >= sz) {
			if (sz > skip) {
				luaL_addlstring(&b, current->msg + sb->offset, sz - skip);
			} 
			sb->offset += sz;
			if (bytes == sz) {
				return_free_node(L,2,sb);
			}
			break;
		}
		int real_sz = sz - skip;
		if (real_sz > 0) {
			luaL_addlstring(&b, current->msg + sb->offset, (real_sz < bytes) ? real_sz : bytes);
		}
		return_free_node(L,2,sb);
		sz-=bytes;
		if (sz==0)
			break;
		current = sb->head;
		assert(current);
	}
	luaL_pushresult(&b);
}

/* [lua_api] 将传入的长度在 1~4 之间的单字节数组以大端形式解释成一个整数. 单字节数组表示一个数据的长度, 附在数据的头部.
 * 参数: string [1] 为数据头部, 是大端形式的整数, 长度在 1~4 之间;
 * 返回: int [1] 是以大端形式读取到的整数; */
static int
lheader(lua_State *L) {
	size_t len;
	const uint8_t * s = (const uint8_t *)luaL_checklstring(L, 1, &len);
	if (len > 4 || len < 1) {
		return luaL_error(L, "Invalid read %s", s);
	}
	int i;
	size_t sz = 0;
	for (i=0;i<(int)len;i++) {
		sz <<= 8;
		sz |= s[i];
	}

	lua_pushinteger(L, (lua_Integer)sz);

	return 1;
}

/*
	userdata send_buffer
	table pool
	integer sz 
 */
/* [lua_api] 从缓存列表中读取指定长度的字符串, 并返回剩余的缓存列表消息的总大小和读取的字符串数据.
 * 参数: userdata [1] 为缓存列表; table [2] 为缓存节点池; int [3] 是读取的大小
 * 返回: string/nil [1] 为读取的指定长度的字符串, 如果读取的长度大于缓存列表的大小, 为 nil ; int [2] 为剩余的缓存消息的总大小; */
static int
lpopbuffer(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	luaL_checktype(L,2,LUA_TTABLE);
	int sz = luaL_checkinteger(L,3);
	if (sb->size < sz || sz == 0) {
		lua_pushnil(L);
	} else {
		pop_lstring(L,sb,sz,0);
		sb->size -= sz;
	}
	lua_pushinteger(L, sb->size);

	return 2;
}

/*
	userdata send_buffer
	table pool
 */
/* [lua_api] 将整个缓存列表中的数据释放, 并将缓存节点归还到缓存池中.
 * 参数: userdata [1] 是缓存列表; table [2] 是缓存池;
 * 函数无返回值 */
static int
lclearbuffer(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	luaL_checktype(L,2,LUA_TTABLE);
	while(sb->head) {
		return_free_node(L,2,sb);
	}
	sb->size = 0;
	return 0;
}

/* [lua_api] 将缓存列表中的所有数据全部读取完, 并以字符串的形式返回.
 * 参数: userdata [1] 是缓存列表; table [2] 是缓存池;
 * 返回: string [1] 读取出来的所有的数据; */
static int
lreadall(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	luaL_checktype(L,2,LUA_TTABLE);
	luaL_Buffer b;
	luaL_buffinit(L, &b);
	while(sb->head) {
		struct buffer_node *current = sb->head;
		luaL_addlstring(&b, current->msg + sb->offset, current->sz - sb->offset);
		return_free_node(L,2,sb);
	}
	luaL_pushresult(&b);
	sb->size = 0;
	return 1;
}

/* [lua_api] 将由 skynet 分配的内存回收, 内存指针是 lightuserdata 类型.
 * 参数: lightuserdata [1] 为需要回收的内存指针; int [2] 是内存大小;
 * 函数无返回值 */
static int
ldrop(lua_State *L) {
	void * msg = lua_touserdata(L,1);
	luaL_checkinteger(L,2);
	skynet_free(msg);
	return 0;
}

/* 从缓存节点 node 的 from 位置开始比较长度为 seplen 的数据是否与分割字符串相同.
 * 其中缓存列表中消息和分割字符串至少是 seplen 长度. 分割字符串可以跨越多个缓存节点;
 *
 * 参数: node 是开始比较的缓存节点; from 是当前缓存节点开始比较的位置; sep 是用于比较的分割字符串; seplen 是比较的长度;
 * 返回: 是否检查到分割字符串 */
static bool
check_sep(struct buffer_node * node, int from, const char *sep, int seplen) {
	for (;;) {
		int sz = node->sz - from;
		if (sz >= seplen) {
			return memcmp(node->msg+from,sep,seplen) == 0;
		}
		if (sz > 0) {
			if (memcmp(node->msg + from, sep, sz)) {
				return false;
			}
		}
		/* [ck]有没有可能其下一个节点是 NULL? 不会,
		 * 因为 i 的最大值是 sb->size - seplen , 保证缓存列表中有足够多的字符用于比较. [/ck] */
		node = node->next;
		sep += sz;
		seplen -= sz;
		from = 0;
	}
}

/*
	userdata send_buffer
	table pool , nil for check
	string sep
 */
/* [lua_api] 从缓存列表中读取一行消息内容或者检查是否存这样一行消息内容, 分割字符串由第三个参数给出.
 * 当第二个参数是 nil 时表示查看, 若为缓存池则是读取. 一行内容可以跨越多个缓存节点, 读出的消息将包含分割字符串.
 *
 * 参数: userdata [1] 是缓存列表; table/nil [2] 为 table 时就是缓存池表示需要读取一行消息内容, 为 nil 时只检查此行是否存在;
 * string [3] 为分割字符串;
 *
 * 返回: bool/string 当确实存在行时, bool 类型将返回 true, string 类型将返回读取的行; 若不存在将返回 nil; */
static int
lreadline(lua_State *L) {
	struct socket_buffer * sb = lua_touserdata(L, 1);
	if (sb == NULL) {
		return luaL_error(L, "Need buffer object at param 1");
	}
	// only check
	bool check = !lua_istable(L, 2);
	size_t seplen = 0;
	const char *sep = luaL_checklstring(L,3,&seplen);
	int i;
	struct buffer_node *current = sb->head;
	if (current == NULL)
		return 0;
	int from = sb->offset;
	int bytes = current->sz - from;
	/* i 表示当前行的长度, 并且它有可能跨越多个缓存节点 */
	for (i=0;i<=sb->size - (int)seplen;i++) {
		if (check_sep(current, from, sep, seplen)) {
			if (check) {
				lua_pushboolean(L,true);
			} else {
				pop_lstring(L, sb, i+seplen, seplen);
				sb->size -= i+seplen;
			}
			return 1;
		}
		++from;
		--bytes;
		if (bytes == 0) {
			current = current->next;
			from = 0;
			if (current == NULL)
				break;
			bytes = current->sz;
		}
	}
	return 0;
}

/* [lua_api] 将一个 lua 字符串类型转化为 skynet 分配的内存块, 并返回指针和大小;
 * 参数: string [1] 需要转换的字符串;
 * 返回: lightuserdata [1] 是转换后的指针; int [2] 是内存块的字节数; */
static int
lstr2p(lua_State *L) {
	size_t sz = 0;
	const char * str = luaL_checklstring(L,1,&sz);
	void *ptr = skynet_malloc(sz);
	memcpy(ptr, str, sz);
	lua_pushlightuserdata(L, ptr);
	lua_pushinteger(L, (int)sz);
	return 2;
}

// for skynet socket

/*
	lightuserdata msg
	integer size

	return type n1 n2 ptr_or_string
*/
/* [lua_api] 将一个从套接字模块中接收到的消息解析成 Lua 能够识别的消息.
 * 参数: lightuserdata [1] 为套接字消息; int [2] 是消息大小;
 *
 * 返回: int [1] 是消息类型; int [2] 是套接字 id; int [3] 当 SKYNET_SOCKET_TYPE_ACCEPT 时 ud 表示在侦听端口上连接上来的套接字连接的 id,
 * 当为 SKYNET_SOCKET_TYPE_DATA/SKYNET_SOCKET_TYPE_UDP 时表示接收到的数据大小, 其它情况下均为 0;
 * string/lightuserdata [4] 是消息内容, 只有在 SKYNET_SOCKET_TYPE_DATA/SKYNET_SOCKET_TYPE_UDP 情况下返回 lightuserdata, 其它情况是 string;
 * string/nil [5] 当 SKYNET_SOCKET_TYPE_UDP 下还可能有对端地址; */
static int
lunpack(lua_State *L) {
	struct skynet_socket_message *message = lua_touserdata(L,1);
	int size = luaL_checkinteger(L,2);

	lua_pushinteger(L, message->type);
	lua_pushinteger(L, message->id);
	lua_pushinteger(L, message->ud);
	if (message->buffer == NULL) {
		/*[ck]有可能 size-sizeof(*message) 是 0, 就是说根本没有消息 [/ck]*/
		lua_pushlstring(L, (char *)(message+1),size - sizeof(*message));
	} else {
		lua_pushlightuserdata(L, message->buffer);
	}
	if (message->type == SKYNET_SOCKET_TYPE_UDP) {
		int addrsz = 0;
		const char * addrstring = skynet_socket_udp_address(message, &addrsz);
		if (addrstring) {
			lua_pushlstring(L, addrstring, addrsz);
			return 5;
		}
	}
	return 4;
}

/* 获取 ip 地址和端口号. 地址 addr 的格式可能是 [ipv6]:port 或者 ipv4:port, 或者端口号在 Lua 虚拟机上, 地址 addr 就只是 ip 地址.
 *
 * 参数: L 是 Lua 虚拟机, 其 port_index 位置的元素可能包含了端口号; tmp 用于从 addr 中分离出 ip 地址; addr 是可能多种形式的地址;
 * port_index 是端口号在 Lua 虚拟机上的栈位置; 出参 port 用于接收端口号;
 *
 * 返回: 主机地址, 当给出的地址格式不正确时时将在 Lua 虚拟机中抛出错误 */
static const char *
address_port(lua_State *L, char *tmp, const char * addr, int port_index, int *port) {
	const char * host;
	if (lua_isnoneornil(L,port_index)) {
		host = strchr(addr, '[');
		if (host) {
			// is ipv6
			++host;
			const char * sep = strchr(addr,']');
			if (sep == NULL) {
				luaL_error(L, "Invalid address %s.",addr);
			}
			memcpy(tmp, host, sep-host);
			tmp[sep-host] = '\0';
			host = tmp;
			sep = strchr(sep + 1, ':');
			if (sep == NULL) {
				luaL_error(L, "Invalid address %s.",addr);
			}
			*port = strtoul(sep+1,NULL,10);
		} else {
			// is ipv4
			const char * sep = strchr(addr,':');
			if (sep == NULL) {
				luaL_error(L, "Invalid address %s.",addr);
			}
			memcpy(tmp, addr, sep-addr);
			tmp[sep-addr] = '\0';
			host = tmp;
			*port = strtoul(sep+1,NULL,10);
		}
	} else {
		host = addr;
		*port = luaL_optinteger(L,port_index, 0);
	}
	return host;
}

/* [lua_api] 从 Lua 虚拟机中发起新的套接字连接请求, 完成之后返回套接字 id 或者错误返回 -1 . 若地址格式错误将抛出错误.
 *
 * 参数: string [1] 为格式是 [ipv6]:port 或者 ipv4:port 形式的地址或者仅包含主机地址;
 * int [2] 可选参数, 当 [1] 仅包含主机地址时此字段需包含一个端口号;
 *
 * 返回: 成功时返回套接字 id, 失败时返回 -1 . 如果地址格式错误将抛出错误; */
static int
lconnect(lua_State *L) {
	size_t sz = 0;
	const char * addr = luaL_checklstring(L,1,&sz);
	char tmp[sz];
	int port = 0;
	const char * host = address_port(L, tmp, addr, 2, &port);
	if (port == 0) {
		return luaL_error(L, "Invalid port");
	}
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = skynet_socket_connect(ctx, host, port);
	lua_pushinteger(L, id);

	return 1;
}

/* [lua_api] 发起关闭套接字的请求. 被关闭的套接字会等待套接字中所有的写缓冲都被写入完成后才会真正的关闭.
 * 参数: int [1] 是需要关闭的套接字 id ;
 * 函数无返回值 */
static int
lclose(lua_State *L) {
	int id = luaL_checkinteger(L,1);
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	skynet_socket_close(ctx, id);
	return 0;
}

/* [lua_api] 立即关闭套接字. 被关闭的套接字将在处理线程接收到命令之后尽可能多的发送写缓冲, 并尽快关闭套接字.
 * 参数: int [1] 是需要关闭的套接字 id ;
 * 函数无返回值 */
static int
lshutdown(lua_State *L) {
	int id = luaL_checkinteger(L,1);
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	skynet_socket_shutdown(ctx, id);
	return 0;
}

/* [lua_api] 侦听一个 ip 地址, 其中主机地址可为空字符串, 此时将侦听 0.0.0.0 . 默认的未完成连接的请求的队列大小为 BACKLOG .
 * 参数: string [1] 是主机名, 可以为空字符串; int [2] 为侦听的端口号; int [3] 为未完成连接的请求的队列大小, 可以为 nil;
 * 返回: 成功时返回套接字 id, 失败时将抛出错误; */
static int
llisten(lua_State *L) {
	const char * host = luaL_checkstring(L,1);
	int port = luaL_checkinteger(L,2);
	int backlog = luaL_optinteger(L,3,BACKLOG);
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = skynet_socket_listen(ctx,host,port,backlog);
	if (id < 0) {
		return luaL_error(L, "Listen error");
	}

	lua_pushinteger(L,id);
	return 1;
}

/* 获取栈位置为 index 的 Lua 字符串数组中的字符串的长度总和. */
static size_t
count_size(lua_State *L, int index) {
	size_t tlen = 0;
	int i;
	for (i=1;lua_geti(L, index, i) != LUA_TNIL; ++i) {
		size_t len;
		luaL_checklstring(L, -1, &len);
		tlen += len;
		lua_pop(L,1);
	}
	lua_pop(L,1);
	return tlen;
}

/* 将栈位置为 index 的 Lua 字符串数组内容拼合到缓存 buffer 中, 要求 tlen 的大小等于字符串数组的内容长度总和.
 * 否则将释放 buffer 的内存并抛出错误 . */
static void
concat_table(lua_State *L, int index, void *buffer, size_t tlen) {
	char *ptr = buffer;
	int i;
	for (i=1;lua_geti(L, index, i) != LUA_TNIL; ++i) {
		size_t len;
		const char * str = lua_tolstring(L, -1, &len);
		if (str == NULL || tlen < len) {
			break;
		}
		memcpy(ptr, str, len);
		ptr += len;
		tlen -= len;
		lua_pop(L,1);
	}
	if (tlen != 0) {
		skynet_free(buffer);
		luaL_error(L, "Invalid strings table");
	}
	lua_pop(L,1);
}

/* 将栈位置在 index 处的多种类型的缓存转换成套接字发送缓存, 发送缓存要求内存必须是堆内存, 并且将 sz 设置为发送缓存的大小.
 * [index] 类型可以为 userdata 、lightuserdata 、table(字符串数组) 或者 string 类型. 如果是用户数据, index 后跟随缓存大小. */
static void *
get_buffer(lua_State *L, int index, int *sz) {
	void *buffer;
	switch(lua_type(L, index)) {
		const char * str;
		size_t len;
	case LUA_TUSERDATA:
	case LUA_TLIGHTUSERDATA:
		buffer = lua_touserdata(L,index);
		*sz = luaL_checkinteger(L,index+1);
		break;
	case LUA_TTABLE:
		// concat the table as a string
		len = count_size(L, index);
		buffer = skynet_malloc(len);
		concat_table(L, index, buffer, len);
		*sz = (int)len;
		break;
	default:
		str =  luaL_checklstring(L, index, &len);
		buffer = skynet_malloc(len);
		memcpy(buffer, str, len);
		*sz = (int)len;
		break;
	}
	return buffer;
}

/* [lua_api] 发送高权限套接字数据, 此数据可以针对 TCP 和 UDP 两个协议的套接字.
 * 参数: int [1] 套接字 id; userdata/lightuserdata/table/string [2] 为多种形式的缓存数据, 如果是用户数据 int [3] 则是数据大小;
 * 返回: 是否成功, 成功时返回 true, 失败时返回 false . */
static int
lsend(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	int sz = 0;
	void *buffer = get_buffer(L, 2, &sz);
	int err = skynet_socket_send(ctx, id, buffer, sz);
	lua_pushboolean(L, !err);
	return 1;
}

/* [lua_api] 发送低权限套接字数据, 此数据可以针对 TCP 和 UDP 两个协议的套接字.
 * 参数: int [1] 套接字 id; userdata/lightuserdata/table/string [2] 为多种形式的缓存数据, 如果是用户数据 int [3] 则是数据大小; */
static int
lsendlow(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	int sz = 0;
	void *buffer = get_buffer(L, 2, &sz);
	skynet_socket_send_lowpriority(ctx, id, buffer, sz);
	return 0;
}

/* [lua_api] 将操作系统套接字描述符 fd 绑定此服务中, 并返回 skynet 系统的套接字 id .
 * 参数: int [1] 是套接字文件描述符;
 * 返回: int [1] skynet 系统中的套接字 id */
static int
lbind(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int fd = luaL_checkinteger(L, 1);
	int id = skynet_socket_bind(ctx,fd);
	lua_pushinteger(L,id);
	return 1;
}

/* [lua_api] 对类型为 SOCKET_TYPE_PACCEPT, SOCKET_TYPE_PLISTEN 和 SOCKET_TYPE_CONNECTED 的套接字发起启动命令.
 * 将导致前两种套接字开始接收 I/O 事件通知, 导致最后一种类型的套接字所有权发生转移. */
static int
lstart(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	skynet_socket_start(ctx,id);
	return 0;
}

/* [lua_api] 设置套接字 id 的非延迟属性 */
static int
lnodelay(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	skynet_socket_nodelay(ctx,id);
	return 0;
}

/* [lua_api] 生成一个 UDP 套接字, 如果提供了主机地址和端口将把此套接字绑定到此地址上. 如果没有则不绑定.
 * 地址的格式可能是 [ipv6]:port 或者 ipv4:port, 或者地址仅包含主机地址, 端口号是其后的参数.
 *
 * 参数: string/nil [1] 绑定的地址或者没有地址; int/nil [2] 当地址仅仅是主机地址时, 此参数为端口号;
 * 返回: 生成成功时的 UDP 套接字 id, 如果失败将抛出错误 */
static int
ludp(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	size_t sz = 0;
	const char * addr = lua_tolstring(L,1,&sz);
	char tmp[sz];
	int port = 0;
	const char * host = NULL;
	if (addr) {
		host = address_port(L, tmp, addr, 2, &port);
	}

	int id = skynet_socket_udp(ctx, host, port);
	if (id < 0) {
		return luaL_error(L, "udp init failed");
	}
	lua_pushinteger(L, id);
	return 1;
}

/* [lua_api] 发起 UDP 连接, 其实质是将由主机和端口标识的地址关联到套接字中去, 需要注意套接字类型要与地址类型一致.
 * 成功之后可以调用 lsend 和 lsendlow 发送消息.
 *
 * 参数: int [1] 为 UDP 套接字 id; string [2] 为连接地址, 具体参见 address_port 描述; int/nil [3] 若连接地址中仅包含主机地址, 此参数为端口号;
 * 函数无返回值, 但连接失败时将抛出错误; */
static int
ludp_connect(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	size_t sz = 0;
	const char * addr = luaL_checklstring(L,2,&sz);
	char tmp[sz];
	int port = 0;
	const char * host = NULL;
	if (addr) {
		host = address_port(L, tmp, addr, 3, &port);
	}

	if (skynet_socket_udp_connect(ctx, id, host, port)) {
		return luaL_error(L, "udp connect failed");
	}

	return 0;
}

/* [lua_api] 向指定地址发送高权限 UDP 包. 地址的格式必须是 socket_server 模块内部定义的地址格式.
 *
 * 参数: int [1] 是套接字 id; string [2] 是对端地址, 格式为 socket_server 模块内部定义的格式;
 * userdata/lightuserdata/table/string [3] 为多种形式的缓存数据, 如果是用户数据 int [4] 则是数据大小;
 *
 * 返回: 是否成功, 成功时返回 true, 失败时返回 false . */
static int
ludp_send(lua_State *L) {
	struct skynet_context * ctx = lua_touserdata(L, lua_upvalueindex(1));
	int id = luaL_checkinteger(L, 1);
	const char * address = luaL_checkstring(L, 2);
	int sz = 0;
	void *buffer = get_buffer(L, 3, &sz);
	int err = skynet_socket_udp_send(ctx, id, address, buffer, sz);

	lua_pushboolean(L, !err);

	return 1;
}

/* [lua_api] 将 socket_server 模块内部定义的地址格式转化为 lua 能够识别的地址. 这种特定的数据格式是头字节是协议类型,
 * 可能为 UDP 或者 UDPv6 两种, 接下来是两个字节是端口号, 剩下的为 ip , ip 可能是 ipv4(32bit) 也可能是 ipv6(128bit)
 *
 * 参数: string [1] 长度为 1+2+4 或者 1+2+16 的地址;
 * 返回: string [1] 为主机地址, ipv4 或 ipv6 形式; int [2] 是端口号; */
static int
ludp_address(lua_State *L) {
	size_t sz = 0;
	const uint8_t * addr = (const uint8_t *)luaL_checklstring(L, 1, &sz);
	uint16_t port = 0;
	memcpy(&port, addr+1, sizeof(uint16_t));
	port = ntohs(port);
	const void * src = addr+3;
	char tmp[256];
	int family;
	if (sz == 1+2+4) {
		family = AF_INET;
	} else {
		if (sz != 1+2+16) {
			return luaL_error(L, "Invalid udp address");
		}
		family = AF_INET6;
	}
	if (inet_ntop(family, src, tmp, sizeof(tmp)) == NULL) {
		return luaL_error(L, "Invalid udp address");
	}
	lua_pushstring(L, tmp);
	lua_pushinteger(L, port);
	return 2;
}

/* 将 C 函数注册为 Lua 函数, 其中与缓存相关的函数不共享服务上下文上值, 而套接字相关函数则会共享此上值. */
int
luaopen_socketdriver(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "buffer", lnewbuffer },
		{ "push", lpushbuffer },
		{ "pop", lpopbuffer },
		{ "drop", ldrop },
		{ "readall", lreadall },
		{ "clear", lclearbuffer },
		{ "readline", lreadline },
		{ "str2p", lstr2p },
		{ "header", lheader },
		{ "unpack", lunpack },
		{ NULL, NULL },
	};
	luaL_newlib(L,l);
	luaL_Reg l2[] = {
		{ "connect", lconnect },
		{ "close", lclose },
		{ "shutdown", lshutdown },
		{ "listen", llisten },
		{ "send", lsend },
		{ "lsend", lsendlow },
		{ "bind", lbind },
		{ "start", lstart },
		{ "nodelay", lnodelay },
		{ "udp", ludp },
		{ "udp_connect", ludp_connect },
		{ "udp_send", ludp_send },
		{ "udp_address", ludp_address },
		{ NULL, NULL },
	};
	lua_getfield(L, LUA_REGISTRYINDEX, "skynet_context");
	struct skynet_context *ctx = lua_touserdata(L,-1);
	if (ctx == NULL) {
		return luaL_error(L, "Init skynet context first");
	}

	luaL_setfuncs(L,l2,1);

	return 1;
}
