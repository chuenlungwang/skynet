#include "skynet_malloc.h"

#include "skynet_socket.h"

#include <lua.h>
#include <lauxlib.h>

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define QUEUESIZE 1024
#define HASHSIZE 4096
#define SMALLSTRING 2048

#define TYPE_DATA 1
#define TYPE_MORE 2
#define TYPE_ERROR 3
#define TYPE_OPEN 4
#define TYPE_CLOSE 5
#define TYPE_WARNING 6

/*
	Each package is uint16 + data , uint16 (serialized in big-endian) is the number of bytes comprising the data .
 */

/* 由客户端发送过来的数据包, 其结构是头两个字节是大端形式的 16 位数字, 表示其后的数据的长度. */
struct netpack {
	int id;          /* 套接字的 id */
	int size;        /* 缓冲数据的大小 */
	void * buffer;   /* 缓冲数据内容 */
};

/* 未完全接收数据包 */
struct uncomplete {
	struct netpack pack;          /* 数据包, 包中的缓冲数据已经预先分配足以容纳完整数据的内存, 随着不断读取而将数据复制进去 */
	struct uncomplete * next;     /* 在未完全接收数据包哈希表中处于同一索引位置的下一个节点, 它们串接起来构成一个链表 */
	int read;                     /* 当前已经读取的数据内容, 下次复制的起点将从 pack.buffer+read 开始写入, 特别是当前读取到的数据只有一个字节时,
	                               * 不足以完全表示数据的长度, 因而此时 read 将为 -1 */
	int header;                   /* 如果当前读取到的数据只有一个字节, 此字段为那个字节的数据内容, 用于后续构造数据包的大小 */
};

/* 包队列, 包含已经成功接收的消息和未完全接收的消息 */
struct queue {
	int cap;        /* 队列的容量, 为成功接收的消息队列的长度, 它会在满的情况下扩大, 扩展的容量在本结构的尾部 */
	int head;       /* 头部, 为已经读取到的位置, 会随着逐渐读取而推进, 直到等于尾部 */
	int tail;       /* 尾部, 为最终写到的位置, 会随着逐渐写入而推进, 直到重新回绕并等于头部, 此时将扩展队列 */
	struct uncomplete * hash[HASHSIZE];  /* 未完全接收数据包的哈希表, 将套接字 id 进行哈希定位到某个节点, 并将定位在一处的消息链接起来 */
	struct netpack queue[QUEUESIZE];     /* 存放完全接收的消息的队列, 当队列满员之后将执行扩展 */
};

/* 清理一条未完成的数据包链表, 函数依次释放数据缓冲以及未完成的包结构的内存.
 * 参数: uc 为未完成的数据包的链表的头结点;
 * 函数无返回值. */
static void
clear_list(struct uncomplete * uc) {
	while (uc) {
		skynet_free(uc->pack.buffer);
		void * tmp = uc;
		uc = uc->next;
		skynet_free(tmp);
	}
}

/* [lua_api] 将包队列中的消息全部清除, 并回收它们的内存. 这些消息包括未完成的消息和已经成功接收的消息.
 * 此函数常用于表的元表中的 __gc 方法.
 *
 * 参数: userdata[1] 为待清除的包队列, 有可能为 NULL.
 * 函数无返回值 */
static int
lclear(lua_State *L) {
	/* queue 本身的内存由 Lua 管理, 是完全用户数据 */
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL) {
		return 0;
	}
	int i;
	for (i=0;i<HASHSIZE;i++) {
		clear_list(q->hash[i]);
		q->hash[i] = NULL;
	}
	if (q->head > q->tail) {
		q->tail += q->cap;
	}
	for (i=q->head;i<q->tail;i++) {
		struct netpack *np = &q->queue[i % q->cap];
		skynet_free(np->buffer);
	}
	q->head = q->tail = 0;

	return 0;
}

/* 对套接字的 id 进行哈希求值. 方法是将此整数向右移动 24 位加上移动 12 位加上本身, 并最终对 HASHSIZE 求模. */
static inline int
hash_fd(int fd) {
	int a = fd >> 24;
	int b = fd >> 12;
	int c = fd;
	return (int)(((uint32_t)(a + b + c)) % HASHSIZE);
}

/* 从包队列中查找套接字 fd 的未完全接收的数据包. 查找到之后还会将其从中取出. 如果未找到将返回 NULL. */
static struct uncomplete *
find_uncomplete(struct queue *q, int fd) {
	if (q == NULL)
		return NULL;
	int h = hash_fd(fd);
	struct uncomplete * uc = q->hash[h];
	if (uc == NULL)
		return NULL;
	if (uc->pack.id == fd) {
		q->hash[h] = uc->next;
		return uc;
	}
	struct uncomplete * last = uc;
	while (last->next) {
		uc = last->next;
		if (uc->pack.id == fd) {
			last->next = uc->next;
			return uc;
		}
		last = uc;
	}
	return NULL;
}

/* 从虚拟机栈位置一处获得包队列, 如果不存在相应的包队列将创建一个并放到位置一. */
static struct queue *
get_queue(lua_State *L) {
	struct queue *q = lua_touserdata(L,1);
	if (q == NULL) {
		q = lua_newuserdata(L, sizeof(struct queue));
		q->cap = QUEUESIZE;
		q->head = 0;
		q->tail = 0;
		int i;
		for (i=0;i<HASHSIZE;i++) {
			q->hash[i] = NULL;
		}
		lua_replace(L, 1);
	}
	return q;
}

/* 扩展包队列, 并放到位置一处, 扩展后的大小比原来大 QUEUESIZE. 并且其内容将复制到新队列的头部. 而旧的包队列将被重置回原始的状态.
 * 如果在 Lua 中没有保持旧数据包队列的话, 它的内存将会被回收. */
static void
expand_queue(lua_State *L, struct queue *q) {
	struct queue *nq = lua_newuserdata(L, sizeof(struct queue) + q->cap * sizeof(struct netpack));
	nq->cap = q->cap + QUEUESIZE;
	nq->head = 0;
	nq->tail = q->cap;
	memcpy(nq->hash, q->hash, sizeof(nq->hash));
	memset(q->hash, 0, sizeof(q->hash));
	int i;
	for (i=0;i<q->cap;i++) {
		int idx = (q->head + i) % q->cap;
		nq->queue[i] = q->queue[idx];
	}
	q->head = q->tail = 0;
	lua_replace(L,1);
}

/* 向位于虚拟机栈位置一的包队列中插入一个完整的数据包. 当队列不够容纳此包时将扩张队列. clone 表示是否需要复制消息.
 * 参数: L 是 Lua 虚拟机栈; fd 是套接字 id; buffer 是数据缓冲; size 是数据的大小; clone 表示是否需要复制消息. */
static void
push_data(lua_State *L, int fd, void *buffer, int size, int clone) {
	if (clone) {
		void * tmp = skynet_malloc(size);
		memcpy(tmp, buffer, size);
		buffer = tmp;
	}
	struct queue *q = get_queue(L);
	struct netpack *np = &q->queue[q->tail];
	if (++q->tail >= q->cap)
		q->tail -= q->cap;
	np->id = fd;
	np->buffer = buffer;
	np->size = size;
	if (q->head == q->tail) {
		expand_queue(L, q);
	}
}

/* 向位于虚拟机栈位置一的包队列中插入一个不完整的包. 并返回这个包的地址, 此时包中还没有内容.
 * 插入的位置遵照 fd 的哈希值获得.
 *
 * 参数: L 是 Lua 虚拟机栈; fd 是套接字 id;
 * 返回: 新生成的不完整包对象 */
static struct uncomplete *
save_uncomplete(lua_State *L, int fd) {
	struct queue *q = get_queue(L);
	int h = hash_fd(fd);
	struct uncomplete * uc = skynet_malloc(sizeof(struct uncomplete));
	memset(uc, 0, sizeof(*uc));
	uc->next = q->hash[h];
	uc->pack.id = fd;
	q->hash[h] = uc;

	return uc;
}

/* 以大端形式从缓冲中读取两个字节表示的长度. 函数要求 buffer 数组的长度至少是 2 个字节.
 * 参数: buffer 是头两个字节为大端形式整数的数据缓冲;
 * 返回: 长度值 */
static inline int
read_size(uint8_t * buffer) {
	int r = (int)buffer[0] << 8 | (int)buffer[1];
	return r;
}

/* 向数据包队列中插入数据, 这些数据会按照数据包的结构, 即两个字节的长度紧跟着内容, 分析这些数据有多少个数据包.
 * 对于其中的完整数据包将插入到完整包队列中去, 对于其中不完整的包将插入到不完整数据包的哈希表中去. 特别是对于
 * 长度只有 1 个字节的不完整包, 此时已经无法计算出数据包的长度, 因而不完整数据包的 read 为 -1 , 而 header 为此一个字节值.
 * 当下一次套接字中接收到数据时会判断 read 的长度, 从而正确拼接数据包.
 *
 * 参数: L 是虚拟机栈; fd 是数据所属的套接字 id; buffer 是数据内容; size 是数据大小;
 * 函数无返回值 */
static void
push_more(lua_State *L, int fd, uint8_t *buffer, int size) {
	if (size == 1) {
		struct uncomplete * uc = save_uncomplete(L, fd);
		uc->read = -1;
		uc->header = *buffer;
		return;
	}
	int pack_size = read_size(buffer);
	buffer += 2;
	size -= 2;

	/* 虽然数据包的内容是不完整的, 但是给 pack.buffer 分配的内存是完整的 */
	if (size < pack_size) {
		struct uncomplete * uc = save_uncomplete(L, fd);
		uc->read = size;
		uc->pack.size = pack_size;
		uc->pack.buffer = skynet_malloc(pack_size);
		memcpy(uc->pack.buffer, buffer, size);
		return;
	}
	push_data(L, fd, buffer, pack_size, 1);

	buffer += pack_size;
	size -= pack_size;
	if (size > 0) {
		push_more(L, fd, buffer, size);
	}
}

/* 删除与套接字 fd 相关的不完整的数据包. 这个函数只会在套接字发生错误或者关闭时调用.
 * 参数: L 是 Lua 虚拟机栈, 其位置一就是 queue 数据结构; fd 是套接字 id; */
static void
close_uncomplete(lua_State *L, int fd) {
	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		skynet_free(uc->pack.buffer);
		skynet_free(uc);
	}
}

/* [lua_api] 将套接字发送过来的数据返回给 Lua 层使用. 函数首先将检查 queue 的不完整数据包哈希表中是否存在套接字 fd 的
 * 不完整数据包, 如果有则检查其已经读取的内容长度, 并将剩余部分复制过去, 如果刚好是一个数据包大小就直接返回 "data" 字符
 * 串和数据包给 Lua 层, 如果复制后多于一个数据包, 则将这些数据包一起插入到 queue 中, 并返回 "more" 字符串给 Lua 层.
 * 在不足一个数据包的情况下, 将不完整包重新插入到哈希表中. 对于之前没有不完整包的情况执行相同的流程. 当返回 "more" 时,
 * 可以通过 lpop 函数取得 queue 中的数据包. buffer 中的内容会复制到新的内存块中, 因而在调用完此函数之后需要释放 buffer 的内存;
 *
 * 参数: L 是虚拟机栈, 其位置一就是 queue 数据结构; fd 是套接字 id; buffer 是数据内容; size 是数据大小;
 *
 * 返回: userdata[1] 为 queue 数据结构; string/nil[2] 为 "more" 或者 "data" 表示有数据, nil 表示数据不完整;
 *       int[3] 为套接字 id, 仅在 "data" 下返回; lightuserdata[4] 为数据内容, 仅在 "data" 下返回;
 *       int[5] 为数据大小, 仅在 "data" 下返回; */
static int
filter_data_(lua_State *L, int fd, uint8_t * buffer, int size) {
	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		// fill uncomplete
		if (uc->read < 0) {
			// read size
			assert(uc->read == -1);
			int pack_size = *buffer;
			pack_size |= uc->header << 8 ;
			++buffer;
			--size;
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			uc->read = 0;
		}
		int need = uc->pack.size - uc->read;
		if (size < need) {
			memcpy(uc->pack.buffer + uc->read, buffer, size);
			uc->read += size;
			int h = hash_fd(fd);
			uc->next = q->hash[h];
			q->hash[h] = uc;
			return 1;
		}
		memcpy(uc->pack.buffer + uc->read, buffer, need);
		buffer += need;
		size -= need;
		if (size == 0) {
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			lua_pushlightuserdata(L, uc->pack.buffer);
			lua_pushinteger(L, uc->pack.size);
			skynet_free(uc);
			return 5;
		}
		// more data
		push_data(L, fd, uc->pack.buffer, uc->pack.size, 0);
		skynet_free(uc);
		push_more(L, fd, buffer, size);
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	} else {
		if (size == 1) {
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = -1;
			uc->header = *buffer;
			return 1;
		}
		int pack_size = read_size(buffer);
		buffer+=2;
		size-=2;

		if (size < pack_size) {
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = size;
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			memcpy(uc->pack.buffer, buffer, size);
			return 1;
		}
		if (size == pack_size) {
			// just one package
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			void * result = skynet_malloc(pack_size);
			memcpy(result, buffer, size);
			lua_pushlightuserdata(L, result);
			lua_pushinteger(L, size);
			return 5;
		}
		// more data
		push_data(L, fd, buffer, pack_size, 1);
		buffer += pack_size;
		size -= pack_size;
		push_more(L, fd, buffer, size);
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	}
}

/* 将套接字发送过来的数据分割成一个个数据包并返回给 Lua 层. 具体工作及返回给 Lua 值参见 filter_data_ 函数.
 * 参数: L 是虚拟机栈, 其位置一就是 queue 数据结构; fd 是套接字 id; buffer 是数据内容; size 是数据大小;
 * 返回: 返回给 Lua 层的值数目 */
static inline int
filter_data(lua_State *L, int fd, uint8_t * buffer, int size) {
	int ret = filter_data_(L, fd, buffer, size);
	// buffer is the data of socket message, it malloc at socket_server.c : function forward_message .
	// it should be free before return,
	skynet_free(buffer);
	return ret;
}

/* 将内存块中的数据压栈为 Lua 中的字符串, 如果内存块为 NULL 则转为空字符串.
 * 参数: L 是虚拟机栈; msg 为 C 中的内存地址; size 是内存块的大小; */
static void
pushstring(lua_State *L, const char * msg, int size) {
	if (msg) {
		lua_pushlstring(L, msg, size);
	} else {
		lua_pushliteral(L, "");
	}
}

/*
	userdata queue
	lightuserdata msg
	integer size
	return
		userdata queue
		string type
		integer fd
		string msg | lightuserdata/integer
 */
/* [lua_api] 将收到的套接字消息转化为 Lua 层能够识别的消息. 当接收到数据包时返回 "data" 字符串以及相应的数据, 返回 "more" 表示数据被
 * 插入到 queue 数据结构中. 当套接字关闭时返回 "close" 和关闭的套接字 id; 当有新的连接到来时将返回 "open" 和套接字 id 以及客户端 ip;
 * 当套接字发生错误时将返回 "error" 以及套接字 id 和错误消息; 当套接字产生警告消息时将返回 "warning"和套接字 id 以及写缓冲的大小(KB 为单位);
 *
 * 参数: userdata[1] 为 queue 数据结构; lightuserdata[2] 是消息体; int[3] 为消息大小;
 * 返回: userdata[1] 为 queue 数据结构; string[2] 是消息类型; int[3] 是套接字 id; string[4] | lightuserdata[4]/int[5] 是消息内容; */
static int
lfilter(lua_State *L) {
	struct skynet_socket_message *message = lua_touserdata(L,2);
	int size = luaL_checkinteger(L,3);
	char * buffer = message->buffer;
	if (buffer == NULL) {
		buffer = (char *)(message+1);
		size -= sizeof(*message);
	} else {
		size = -1;
	}

	lua_settop(L, 1);

	switch(message->type) {
	case SKYNET_SOCKET_TYPE_DATA:
		// ignore listen id (message->id)
		assert(size == -1);	// never padding string
		return filter_data(L, message->id, (uint8_t *)buffer, message->ud);
	case SKYNET_SOCKET_TYPE_CONNECT:
		// ignore listen fd connect
		return 1;
	case SKYNET_SOCKET_TYPE_CLOSE:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_CLOSE));
		lua_pushinteger(L, message->id);
		return 3;
	case SKYNET_SOCKET_TYPE_ACCEPT:
		lua_pushvalue(L, lua_upvalueindex(TYPE_OPEN));
		// ignore listen id (message->id);
		lua_pushinteger(L, message->ud);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_ERROR:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
		lua_pushinteger(L, message->id);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_WARNING:
		lua_pushvalue(L, lua_upvalueindex(TYPE_WARNING));
		lua_pushinteger(L, message->id);
		lua_pushinteger(L, message->ud);
		return 4;
	default:
		// never get here
		return 1;
	}
}

/*
	userdata queue
	return
		integer fd
		lightuserdata msg
		integer size
 */
/* [lua_api] 从 queue 结构中取出一个完整的数据包. 如果 queue 对象不存在, 或者里边已经没有完整的数据包时返回 nil.
 * 参数: userdata[1] 为 queue 数据结构;
 * 返回: 当无更多数据包时返回 nil; int[1] 为数据包所属的套接字 id; msg[2] 为完整的数据包内容; int[3] 为数据大小; */
static int
lpop(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL || q->head == q->tail)
		return 0;
	struct netpack *np = &q->queue[q->head];
	if (++q->head >= q->cap) {
		q->head = 0;
	}
	lua_pushinteger(L, np->id);
	lua_pushlightuserdata(L, np->buffer);
	lua_pushinteger(L, np->size);

	return 3;
}

/*
	string msg | lightuserdata/integer

	lightuserdata/integer
 */
/* 将位置在 index 处的字符串或者轻量用户数据转化为字符串指针. 如果是轻量用户数据还将提供数据大小.
 * 参数: L 是 Lua 虚拟机栈; 出参 sz 用于接收数据大小; index 是字符串或者轻量用户数据所在的位置;
 * 返回: 转换后的字符串指针 */
static const char *
tolstring(lua_State *L, size_t *sz, int index) {
	const char * ptr;
	if (lua_isuserdata(L,index)) {
		ptr = (const char *)lua_touserdata(L,index);
		*sz = (size_t)luaL_checkinteger(L, index+1);
	} else {
		ptr = luaL_checklstring(L, index, sz);
	}
	return ptr;
}

/* 以大端形式将整数 len 写入到缓冲 buffer 的头两个字节中. 函数要求缓冲至少有两个字节. */
static inline void
write_size(uint8_t * buffer, int len) {
	buffer[0] = (len >> 8) & 0xff;
	buffer[1] = len & 0xff;
}

/* [lua_api] 将字符串或者轻量用户数据打包成数据包. 如果数据的长度大于 0x10000 将抛出错误.
 * 参数: string[1] | lightuserdata[1]/int[2] 字符串或者轻量用户数据以及其大小
 * 返回: lightuserdata[1]/int[2] 为打包好的数据包以及其大小; */
static int
lpack(lua_State *L) {
	size_t len;
	const char * ptr = tolstring(L, &len, 1);
	if (len >= 0x10000) {
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)len);
	}

	uint8_t * buffer = skynet_malloc(len + 2);
	write_size(buffer, len);
	memcpy(buffer+2, ptr, len);

	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, len + 2);

	return 2;
}

/* [lua_api] 将轻量用户数据转化为 Lua 中的字符串并且销毁该轻量用户数据的内存.
 * 参数: lightuserdata[1] 为数据内容; int[2] 为数据大小;
 * 返回: 转化后的 Lua 字符串 */
static int
ltostring(lua_State *L) {
	void * ptr = lua_touserdata(L, 1);
	int size = luaL_checkinteger(L, 2);
	if (ptr == NULL) {
		lua_pushliteral(L, "");
	} else {
		lua_pushlstring(L, (const char *)ptr, size);
		skynet_free(ptr);
	}
	return 1;
}

int
luaopen_netpack(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pop", lpop },
		{ "pack", lpack },
		{ "clear", lclear },
		{ "tostring", ltostring },
		{ NULL, NULL },
	};
	luaL_newlib(L,l);

	// the order is same with macros : TYPE_* (defined top)
	lua_pushliteral(L, "data");
	lua_pushliteral(L, "more");
	lua_pushliteral(L, "error");
	lua_pushliteral(L, "open");
	lua_pushliteral(L, "close");
	lua_pushliteral(L, "warning");

	lua_pushcclosure(L, lfilter, 6);
	lua_setfield(L, -2, "filter");

	return 1;
}
