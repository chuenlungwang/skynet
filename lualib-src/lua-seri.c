/*
	modify from https://github.com/cloudwu/lua-serialize
 */

#include "skynet_malloc.h"

#include <lua.h>
#include <lauxlib.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#define TYPE_NIL 0
#define TYPE_BOOLEAN 1
// hibits 0 false 1 true
#define TYPE_NUMBER 2
// hibits 0 : 0 , 1: byte, 2:word, 4: dword, 6: qword, 8 : double
/* 整数的二级类型, 将会左移 3 位并与 TYPE_NUMBER 位与合并, 从而打包到序列化数据中去 */
#define TYPE_NUMBER_ZERO 0
#define TYPE_NUMBER_BYTE 1
#define TYPE_NUMBER_WORD 2
#define TYPE_NUMBER_DWORD 4
#define TYPE_NUMBER_QWORD 6
#define TYPE_NUMBER_REAL 8

#define TYPE_USERDATA 3
#define TYPE_SHORT_STRING 4
// hibits 0~31 : len
#define TYPE_LONG_STRING 5
#define TYPE_TABLE 6

#define MAX_COOKIE 32       /* 合并到类型中的值的最大必须小于 32, 它可能是整数的二级类型, 也可能是字符串的长度, 或布尔值.
                             * 原因在于类型占据了 3 位, 而整个合并后的值在 1 个字节内. */
#define COMBINE_TYPE(t,v) ((t) | (v) << 3)    /* 值与类型合并到一个字节值中, 值从高 4 位开始, 最大值为 31. */

#define BLOCK_SIZE 128      /* 一个写缓存节点能够装载序列化后的数据的大小 */
#define MAX_DEPTH 32        /* 打包的表的深度, 即键对应的值也是表的树深度的最大值 */

/* 序列化数据的写缓存的一个节点, 整个串联起来的链表是最终的打包数据, 最后一个节点的 next 为 NULL, 链表的第一个节点位于栈上. */
struct block {
	struct block * next;
	char buffer[BLOCK_SIZE];
};

/* 写缓存队列用于将 Lua 栈上的数据序列化成二进制值, 最开始时候此队列会有一个节点, 这个节点是在栈上的,
如果这个节点不足以存储整个序列化后的数据, 将继续分配更多的节点. */
struct write_block {
	struct block * head;       /* 写缓存队列的头结点, 它一定存在且一定是在栈上 */
	struct block * current;    /* 写缓存队列的当前节点, 会随着不断写入而步进 ptr 并最终于到达节点末尾
	                            * 而添加新的节点以替换这个 current */
	int len;                   /* 当前写入的长度, 会随着不断写入而增加 */
	int ptr;                   /* 在当前节点的缓存数据的写入起点, 会随着不断写入而偏移 */
};

/* 读缓存用于将二进制值反序列化为 Lua 栈上的值. */
struct read_block {
	char * buffer;     /* 二进制缓存的数据 */
	int len;           /* 缓存的长度, 会随着不断的读取而减小 */
	int ptr;           /* 在缓存数据中的读取起点, 会随着不断读取而偏移 */
};

/* 分配一个新的写缓存节点 */
inline static struct block *
blk_alloc(void) {
	struct block *b = skynet_malloc(sizeof(struct block));
	b->next = NULL;
	return b;
}

/* 将大小为 sz 的缓存数据 buf 写入到缓存链表中. 如果缓存链表的当前节点无法完全写入此数据, 将分配一个新的节点用于写入. */
inline static void
wb_push(struct write_block *b, const void *buf, int sz) {
	const char * buffer = buf;
	if (b->ptr == BLOCK_SIZE) {
_again:
		b->current = b->current->next = blk_alloc();
		b->ptr = 0;
	}
	if (b->ptr <= BLOCK_SIZE - sz) {
		memcpy(b->current->buffer + b->ptr, buffer, sz);
		b->ptr+=sz;
		b->len+=sz;
	} else {
		int copy = BLOCK_SIZE - b->ptr;
		memcpy(b->current->buffer + b->ptr, buffer, copy);
		buffer += copy;
		b->len += copy;
		sz -= copy;
		goto _again;
	}
}

/* 初始化写缓存队列 wb , 函数接收写缓存的一个节点 b, 这个写缓存节点来自于栈上. */
static void
wb_init(struct write_block *wb , struct block *b) {
	wb->head = b;
	assert(b->next == NULL);
	wb->len = 0;
	wb->current = wb->head;
	wb->ptr = 0;
}

/* 释放写缓存链表上的所有的节点, 链接表的头节点是在栈上的, 所以释放内存是从第二个节点开始.
 * 同时, 写缓存会被重置为最初状态. */
static void
wb_free(struct write_block *wb) {
	struct block *blk = wb->head;
	blk = blk->next;	// the first block is on stack
	while (blk) {
		struct block * next = blk->next;
		skynet_free(blk);
		blk = next;
	}
	wb->head = NULL;
	wb->current = NULL;
	wb->ptr = 0;
	wb->len = 0;
}

/* 将二进制缓存数据 buffer 初始化到读缓存中, 同时用缓存数据的长度初始化读缓存的长度. */
static void
rball_init(struct read_block * rb, char * buffer, int size) {
	rb->buffer = buffer;
	rb->len = size;
	rb->ptr = 0;
}

/* 从读缓存中读取长度为 sz 的数据, 读取操作将返回数据块的起点指针. 若数据不足够将返回 NULL.
 * 读操作并不会复制数据, 而是直接返回缓存中的内存地址. 读操作会导致数据长度 len 减少, 并步进读取起点 ptr . */
static void *
rb_read(struct read_block *rb, int sz) {
	if (rb->len < sz) {
		return NULL;
	}

	int ptr = rb->ptr;
	rb->ptr += sz;
	rb->len -= sz;
	return rb->buffer + ptr;
}

/* 向写缓存中写入一个 nil 数据, 实质为写入一个字节长的 TYPE_NIL 数据类型. */
static inline void
wb_nil(struct write_block *wb) {
	uint8_t n = TYPE_NIL;
	wb_push(wb, &n, 1);
}

/* 向写缓存中写入一个布尔类型数据, 实质为写入一个字节长的 TYPE_BOOLEAN 类型和值的合并值, 值从高 4 位开始. */
static inline void
wb_boolean(struct write_block *wb, int boolean) {
	uint8_t n = COMBINE_TYPE(TYPE_BOOLEAN , boolean ? 1 : 0);
	wb_push(wb, &n, 1);
}

/* 向写缓存中写入一个整数值. 函数依据整数的长度先写入一个字节长的 TYPE_NUMBER 类型和长度相关的二级类型的合并值, 再写入对应长度的值.
 * 如果值是 0 将不写入值, 而只是写入二级类型. */
static inline void
wb_integer(struct write_block *wb, lua_Integer v) {
	int type = TYPE_NUMBER;
	if (v == 0) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_ZERO);
		wb_push(wb, &n, 1);
	} else if (v != (int32_t)v) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_QWORD);
		int64_t v64 = v;
		wb_push(wb, &n, 1);
		wb_push(wb, &v64, sizeof(v64));
	} else if (v < 0) {
		int32_t v32 = (int32_t)v;
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
		wb_push(wb, &n, 1);
		wb_push(wb, &v32, sizeof(v32));
	/* 使用 uint8_t 和 uint16_t 序列化整数的原因在于它们可以刚好容纳数值, 而使用有符号的类型则可能会解释为负数. */
	} else if (v<0x100) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_BYTE);
		wb_push(wb, &n, 1);
		uint8_t byte = (uint8_t)v;
		wb_push(wb, &byte, sizeof(byte));
	} else if (v<0x10000) {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_WORD);
		wb_push(wb, &n, 1);
		uint16_t word = (uint16_t)v;
		wb_push(wb, &word, sizeof(word));
	} else {
		uint8_t n = COMBINE_TYPE(type , TYPE_NUMBER_DWORD);
		wb_push(wb, &n, 1);
		uint32_t v32 = (uint32_t)v;
		wb_push(wb, &v32, sizeof(v32));
	}
}

/* 向写缓存中写入一个浮点数值. 函数首先写入一个字节长的 TYPE_NUMBER 类型和二级浮点类型的合并值, 再写入对应的值. */
static inline void
wb_real(struct write_block *wb, double v) {
	uint8_t n = COMBINE_TYPE(TYPE_NUMBER , TYPE_NUMBER_REAL);
	wb_push(wb, &n, 1);
	wb_push(wb, &v, sizeof(v));
}

/* 向写缓存中写入一个指针地址, 函数首先写入一个字节场的 TYPE_USERDATA, 再对应的指针地址. */
static inline void
wb_pointer(struct write_block *wb, void *v) {
	uint8_t n = TYPE_USERDATA;
	wb_push(wb, &n, 1);
	wb_push(wb, &v, sizeof(v));
}

/* 向写缓存中写入一个字符串, 函数会判断字符串的长度,
 * 1) 如果字符串的长度在 31 个字节之内, 则将一个字节的类型 TYPE_SHORT_STRING 和字符串的长度的合并值写入, 然后写入字符串;
 * 2) 如果字符串的长度在 31 ~ 0x10000 字节长度内, 则将一个字节的类型 TYPE_LONG_STRING 和 2 (长度值本身的长度)
 *    的合并值写入, 再写入两个字节的长度值, 最后写入字符串;
 * 3) 如果字符串的长度在 0x10000 字节及以上时, 则将一个字节的类型 TYPE_LONG_STRING 和 4 (长度值本身的长度) 的合并值写入,
 *    再写入 4 个字节的长度值, 最后写入字符串;
 */
static inline void
wb_string(struct write_block *wb, const char *str, int len) {
	if (len < MAX_COOKIE) {
		uint8_t n = COMBINE_TYPE(TYPE_SHORT_STRING, len);
		wb_push(wb, &n, 1);
		if (len > 0) {
			wb_push(wb, str, len);
		}
	} else {
		uint8_t n;
		if (len < 0x10000) {
			n = COMBINE_TYPE(TYPE_LONG_STRING, 2);
			wb_push(wb, &n, 1);
			uint16_t x = (uint16_t) len;
			wb_push(wb, &x, 2);
		} else {
			n = COMBINE_TYPE(TYPE_LONG_STRING, 4);
			wb_push(wb, &n, 1);
			uint32_t x = (uint32_t) len;
			wb_push(wb, &x, 4);
		}
		wb_push(wb, str, len);
	}
}

static void pack_one(lua_State *L, struct write_block *b, int index, int depth);

/* 以数组形式序列化一个表到写缓存中去, 数组的长度是从 1 开始连续的整数索引值的数目.
 * 函数先写入一个字节, 此字节包含了类型 TYPE_TABLE 和数组长度的合并值, 如果数组的长度大于等于 31 ,
 * 则合并值的高 4 位开始是 31 , 并写入一个整数表示数组的长度. 数组长度可以为 0.
 * 应当在调用此函数之后调用 wb_table_hash, 两个合起来一者写入类型,一者写入 nil, 缺一不可. 此函数会保持栈的平衡.
 *
 * 参数: L 为 Lua 虚拟机, 栈上保存了所有需要序列化的数据; wb 为接收序列化后二进制数据的写缓存; index 是当前需要写入的表在栈上的位置, 可以为负数;
 *       depth 是当前表的深度, 当需要序列化的数据在表中时, 每进入一个字表深度加 1 .
 *
 * 返回: 表数组的长度 */
static int
wb_table_array(lua_State *L, struct write_block * wb, int index, int depth) {
	int array_size = lua_rawlen(L,index);
	if (array_size >= MAX_COOKIE-1) {
		uint8_t n = COMBINE_TYPE(TYPE_TABLE, MAX_COOKIE-1);
		wb_push(wb, &n, 1);
		wb_integer(wb, array_size);
	} else {
		uint8_t n = COMBINE_TYPE(TYPE_TABLE, array_size);
		wb_push(wb, &n, 1);
	}

	int i;
	for (i=1;i<=array_size;i++) {
		lua_rawgeti(L,index,i);
		pack_one(L, wb, -1, depth);
		lua_pop(L,1);
	}

	return array_size;
}

/* 以 next 函数遍历表, 并将整数索引在 1 ~ array_size 的值忽略, 而序列化其它的值到写缓存中去. 此函数会保持栈的平衡.
 * 函数会在调用的最后向写缓存中写入一个 nil 值. 此函数的调用需要在 wb_table_array 调用之后.
 *
 * 参数: L 为 Lua 虚拟机; wb 为接收序列化后二进制数据的写缓存; index 是当前序列化的值在栈上的位置;
 *       depth 是当前表的深度, 当需要序列化的数据在表中时, 每进入一个字表深度加 1 . array_size 是调用 wb_table_array 返回的数组大小; */
static void
wb_table_hash(lua_State *L, struct write_block * wb, int index, int depth, int array_size) {
	lua_pushnil(L);
	while (lua_next(L, index) != 0) {
		if (lua_type(L,-2) == LUA_TNUMBER) {
			if (lua_isinteger(L, -2)) {
				lua_Integer x = lua_tointeger(L,-2);
				if (x>0 && x<=array_size) {
					lua_pop(L,1);
					continue;
				}
			}
		}
		pack_one(L,wb,-2,depth);
		pack_one(L,wb,-1,depth);
		lua_pop(L, 1);
	}
	wb_nil(wb);
}

/* 调用表的元方法 __pairs 函数执行表的遍历, 从而将表序列化到写缓存中去. __pairs 函数在 Lua 栈的顶部, 它接收表本身为参数并返回 3 个值.
 * 这 3 个值分别是迭代函数、调用的第一个参数、调用的第二个参数, 每次调用之后需要更新新的第二个参数为第一个返回值. 具体描述参考通用 for 的文档描述.
 * 函数遵从 wb_table 的行为描述: 写入类型, 其数组大小为 0, 以迭代函数遍历每一个键值对分配序列化, 在最末尾写入一个 nil 作为分割符.
 * 栈顶的 __pairs 会在此函数中被弹出.
 *
 * 参数: L 为 Lua 虚拟机, 栈顶为 __pairs 函数; wb 为接收序列化后二进制数据的写缓存; index 是当前序列化的值在栈上的位置;
 *       depth 是当前表的深度, 当需要序列化的数据在表中时, 每进入一个字表深度加 1 . */
static void
wb_table_metapairs(lua_State *L, struct write_block *wb, int index, int depth) {
	uint8_t n = COMBINE_TYPE(TYPE_TABLE, 0);
	wb_push(wb, &n, 1);
	lua_pushvalue(L, index);
	/* 调用后的结果是 ... f t k |顶部 */
	lua_call(L, 1, 3);
	for(;;) {
		lua_pushvalue(L, -2);
		lua_pushvalue(L, -2);
		lua_copy(L, -5, -3);
		/* 调用前为 ... f t f t k |顶部, 调用后为 ... f t nk nv |顶部, 如果返回的下一个键 nk 为 nil 表示无更多元素 */
		lua_call(L, 2, 2);
		int type = lua_type(L, -2);
		if (type == LUA_TNIL) {
			lua_pop(L, 4);
			break;
		}
		pack_one(L, wb, -2, depth);
		pack_one(L, wb, -1, depth);
		lua_pop(L, 1);
	}
	wb_nil(wb);
}

/* 向写缓存中写入一个表, 表既可以包含整数索引的数组, 也可以包含其它类型值为索引的元素. 但索引必须是可序列化的. 函数会保持栈的平衡.
 * 表的序列化分为四步:
 * 1) 写入类型; 首先写入一个字节的类型 TYPE_TABLE 和数组长度的合并值, 如果数组的长度大于等于 31 , 则合并值的高 4 位开始是 31 ,
 *    并写入一个整数表示数组的长度. 数组长度可以为 0;
 * 2) 序列化数组中的值. 数组中的值可以是任何可序列化的类型值, 需要注意的是如果值为表, 深度不能超过 MAX_DEPTH - depth;
 * 3) 序列化非整数索引的键值对; 同样键或值为表, 需要注意深度不能超过 MAX_DEPTH - depth;
 * 4) 在最末尾写入一个 nil 值作为与其它类型的值分割符;
 *
 * 参数: L 为 Lua 虚拟机, 栈上保存了所有需要序列化的数据; wb 为接收序列化后二进制数据的写缓存; index 是当前需要写入的表在栈上的位置, 可以为负数;
 *       depth 是当前表的深度, 当需要序列化的数据在表中时, 每进入一个字表深度加 1 . */
static void
wb_table(lua_State *L, struct write_block *wb, int index, int depth) {
	/* 由于序列化表需要压栈, 所以需要确保栈上至少还有 LUA_MINSTACK 个空位用于压栈 */
	luaL_checkstack(L, LUA_MINSTACK, NULL);
	/* 将负数索引转为正数的原因在于压栈会使得负数索引将不再指向原来的值 */
	if (index < 0) {
		index = lua_gettop(L) + index + 1;
	}
	if (luaL_getmetafield(L, index, "__pairs") != LUA_TNIL) {
		wb_table_metapairs(L, wb, index, depth);
	} else {
		int array_size = wb_table_array(L, wb, index, depth);
		wb_table_hash(L, wb, index, depth, array_size);
	}
}

/* 将 Lua 虚拟机上位置在 index 的值序列化到写缓冲中, 此位置上的数据类型必须是 nil, number, boolean,
 * string, lightuserdata 和 table 中的一种, 否则将抛出错误. 在序列化表时会依次遍历其每个元素, 若元素也是一个表,
 * 将继续序列化子表, 并对深度 depth 自增 1 . 如果当前的深度已经大于最大深度, 将抛出错误. 函数会保持栈的平衡.
 *
 * 参数: L 是 Lua 虚拟机, 保证当序列化完毕之后栈上的值位置不变; b 是写入缓存, 保存序列化后的二进制数据;
 *      index 是当前序列化的值在栈上的位置; depth 则为当前表的深度, 第一次调用时是 0, 随着遍历子表而自增; */
static void
pack_one(lua_State *L, struct write_block *b, int index, int depth) {
	if (depth > MAX_DEPTH) {
		wb_free(b);
		luaL_error(L, "serialize can't pack too depth table");
	}
	int type = lua_type(L,index);
	switch(type) {
	case LUA_TNIL:
		wb_nil(b);
		break;
	case LUA_TNUMBER: {
		if (lua_isinteger(L, index)) {
			lua_Integer x = lua_tointeger(L,index);
			wb_integer(b, x);
		} else {
			lua_Number n = lua_tonumber(L,index);
			wb_real(b,n);
		}
		break;
	}
	case LUA_TBOOLEAN: 
		wb_boolean(b, lua_toboolean(L,index));
		break;
	case LUA_TSTRING: {
		size_t sz = 0;
		const char *str = lua_tolstring(L,index,&sz);
		wb_string(b, str, (int)sz);
		break;
	}
	case LUA_TLIGHTUSERDATA:
		wb_pointer(b, lua_touserdata(L,index));
		break;
	case LUA_TTABLE: {
		if (index < 0) {
			index = lua_gettop(L) + index + 1;
		}
		wb_table(L, b, index, depth+1);
		break;
	}
	default:
		wb_free(b);
		luaL_error(L, "Unsupport type %s to serialize", lua_typename(L, type));
	}
}

/* 将从虚拟机栈上的位置 from+1 开始直到栈顶的所有值序列化成二进制数据到写缓存中去. 此函数会保持栈的平衡.
 * 栈上值的类型必须是 nil, number, boolean, string, lightuserdata 和 table 中的一种, 否则将抛出错误.
 * 如果栈上的表嵌套过深将无法完成序列化并抛出错误.
 *
 * 参数: L 是 Lua 虚拟机, 保证当序列化完毕之后栈上的值位置不变; b 是写入缓存, 保存序列化后的二进制数据;
 *       from 是指示序列化的起点为 from+1 ; */
static void
pack_from(lua_State *L, struct write_block *b, int from) {
	int n = lua_gettop(L) - from;
	int i;
	for (i=1;i<=n;i++) {
		pack_one(L, b , from + i, 0);
	}
}

/* 抛出错误用于指示错误的 Lua 值序列化后的二进制缓存数据. 函数将指出二进制数据的长度和处理的代码行. */
static inline void
invalid_stream_line(lua_State *L, struct read_block *rb, int line) {
	int len = rb->len;
	luaL_error(L, "Invalid serialize stream %d (line:%d)", len, line);
}

#define invalid_stream(L,rb) invalid_stream_line(L,rb,__LINE__)

/* 从二进制读缓存中读取一个二级类型为 cookie 的整数值. 如果读缓存中没有足够的数据可读, 或者二级类型不正确, 将抛出错误到 Lua 虚拟机中.
 * 参数: L 是 Lua 虚拟机, 它是发起调用的虚拟机; rb 是保存二进制数据的读缓存; cookie 是数值的二级类型;
 * 返回: 读取到的整数, 或抛出错误 */
static lua_Integer
get_integer(lua_State *L, struct read_block *rb, int cookie) {
	/* 单字节和双字节的数值用无符号表示, 因为在于所有的负数都用 int32_t 和 int64_t 类型序列化.
     * 如果用作 int8_t 或者 int16_t 的, 数值就有可能会被解释为负数, 从而出错 */
	switch (cookie) {
	case TYPE_NUMBER_ZERO:
		return 0;
	case TYPE_NUMBER_BYTE: {
		uint8_t n;
		uint8_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		n = *pn;
		return n;
	}
	case TYPE_NUMBER_WORD: {
		uint16_t n;
		uint16_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	case TYPE_NUMBER_DWORD: {
		int32_t n;
		int32_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	case TYPE_NUMBER_QWORD: {
		int64_t n;
		int64_t * pn = rb_read(rb,sizeof(n));
		if (pn == NULL)
			invalid_stream(L,rb);
		memcpy(&n, pn, sizeof(n));
		return n;
	}
	default:
		invalid_stream(L,rb);
		return 0;
	}
}

/* 从二进制读缓存中读取一个浮点数值. 如果读缓存中没有足够的数据可读, 将抛出错误到 Lua 虚拟机中. */
static double
get_real(lua_State *L, struct read_block *rb) {
	double n;
	double * pn = rb_read(rb,sizeof(n));
	if (pn == NULL)
		invalid_stream(L,rb);
	memcpy(&n, pn, sizeof(n));
	return n;
}

/* 从二进制读缓存中读取一个指针值. 如果读缓存中没有足够的数据可读, 将抛出错误到 Lua 虚拟机中. */
static void *
get_pointer(lua_State *L, struct read_block *rb) {
	void * userdata = 0;
	void ** v = (void **)rb_read(rb,sizeof(userdata));
	if (v == NULL) {
		invalid_stream(L,rb);
	}
	memcpy(&userdata, v, sizeof(userdata));
	return userdata;
}

/* 从二进制读缓存中读取一个长度 len 的二进制数据并作为字符串压栈到 Lua 虚拟机上.
 * 如果读缓存中没有足够的数据可读, 将抛出错误到 Lua 虚拟机中. */
static void
get_buffer(lua_State *L, struct read_block *rb, int len) {
	char * p = rb_read(rb,len);
	if (p == NULL) {
		invalid_stream(L,rb);
	}
	lua_pushlstring(L,p,len);
}

static void unpack_one(lua_State *L, struct read_block *rb);

/* 从二进制读缓存中解包一个表, 表的键或值都有可能是一个表. 最终得到的表将压栈到 Lua 虚拟机栈上.
 * 如果在任何时候发现二进制数据错误, 将向 Lua 虚拟机抛出错误. 参数 array_size 是表中序列的大小, 与打包时一样的含义. */
static void
unpack_table(lua_State *L, struct read_block *rb, int array_size) {
	/* 如果 array_size 为 MAX_COOKIE-1 则表示长度在接下来的一个整数值中. */
	if (array_size == MAX_COOKIE-1) {
		uint8_t type;
		uint8_t *t = rb_read(rb, sizeof(type));
		if (t==NULL) {
			invalid_stream(L,rb);
		}
		type = *t;
		int cookie = type >> 3;
		if ((type & 7) != TYPE_NUMBER || cookie == TYPE_NUMBER_REAL) {
			invalid_stream(L,rb);
		}
		array_size = get_integer(L,rb,cookie);
	}
	/* 每次解包一个表时, 都需要向栈上压栈一个新的表, 不断的嵌套将有可能导致栈溢出, 因而需要确保足够的空闲位置. */
	luaL_checkstack(L,LUA_MINSTACK,NULL);
	lua_createtable(L,array_size,0);
	int i;
	for (i=1;i<=array_size;i++) {
		unpack_one(L,rb);
		lua_rawseti(L,-2,i);
	}
	/* 剩余的值是 0 到多对键值对, 并以 nil 结束的. */
	for (;;) {
		unpack_one(L,rb);
		if (lua_isnil(L,-1)) {
			lua_pop(L,1);
			return;
		}
		unpack_one(L,rb);
		lua_rawset(L,-3);
	}
}

/* 从二进制读缓存中读取一个类型为 type, 其高 4 位开始的合并值为 cookie 的数据, 并压栈到 Lua 虚拟机上.
 * cookie 的具体含义由各自类型的序列化函数决定. 当任何时候检查到数据不符合序列化格式将向 Lua 虚拟机抛出错误. */
static void
push_value(lua_State *L, struct read_block *rb, int type, int cookie) {
	switch(type) {
	case TYPE_NIL:
		lua_pushnil(L);
		break;
	case TYPE_BOOLEAN:
		lua_pushboolean(L,cookie);
		break;
	case TYPE_NUMBER:
		if (cookie == TYPE_NUMBER_REAL) {
			lua_pushnumber(L,get_real(L,rb));
		} else {
			lua_pushinteger(L, get_integer(L, rb, cookie));
		}
		break;
	case TYPE_USERDATA:
		lua_pushlightuserdata(L,get_pointer(L,rb));
		break;
	case TYPE_SHORT_STRING:
		get_buffer(L,rb,cookie);
		break;
	case TYPE_LONG_STRING: {
		if (cookie == 2) {
			uint16_t *plen = rb_read(rb, 2);
			if (plen == NULL) {
				invalid_stream(L,rb);
			}
			uint16_t n;
			memcpy(&n, plen, sizeof(n));
			get_buffer(L,rb,n);
		} else {
			if (cookie != 4) {
				invalid_stream(L,rb);
			}
			uint32_t *plen = rb_read(rb, 4);
			if (plen == NULL) {
				invalid_stream(L,rb);
			}
			uint32_t n;
			memcpy(&n, plen, sizeof(n));
			get_buffer(L,rb,n);
		}
		break;
	}
	case TYPE_TABLE: {
		unpack_table(L,rb,cookie);
		break;
	}
	default: {
		invalid_stream(L,rb);
		break;
	}
	}
}

/* 从二进制读缓存中读取一个值, 并压栈到 Lua 虚拟机上. 当任何时候检查到数据不符合序列化格式将向 Lua 虚拟机抛出错误. */
static void
unpack_one(lua_State *L, struct read_block *rb) {
	uint8_t type;
	uint8_t *t = rb_read(rb, sizeof(type));
	if (t==NULL) {
		invalid_stream(L, rb);
	}
	type = *t;
	/* 真实的类型在低 3 位, 而其 cookie 值从高 4 位开始. */
	push_value(L, rb, type & 0x7, type>>3);
}

/* 将写缓存中数据拷贝到一个内存块中, 并把内存块的指针和大小压栈到 Lua 虚拟机上.
 * 参数: b 为写缓存, 其每一个节点都包含了序列化后的二进制数据; len 是整个的二进制数据的总长度; */
static void
seri(lua_State *L, struct block *b, int len) {
	uint8_t * buffer = skynet_malloc(len);
	uint8_t * ptr = buffer;
	int sz = len;
	while(len>0) {
		if (len >= BLOCK_SIZE) {
			memcpy(ptr, b->buffer, BLOCK_SIZE);
			ptr += BLOCK_SIZE;
			len -= BLOCK_SIZE;
			b = b->next;
		} else {
			memcpy(ptr, b->buffer, len);
			break;
		}
	}
	
	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, sz);
}

/* [lua_api] 将二进制数据重新反序列化为 Lua 中的对象. 二进制数据以字符串形式或者用户数据及长度的形式传入此函数.
 * 反序列化之后的值将被压栈到 Lua 虚拟机栈上, 其顺序与序列化是一样的, 返回值的顺序与序列化的参数顺序也是一致的.
 * 当解包失败时将抛出错误.
 *
 * 参数: string/userdata 为需要解包的二进制数据; int 仅当二进制数据是用户数据时提供, 用于指示数据的长度;
 * 返回: 解包出来的 Lua 数据, 如果没有数据将返回 nil, 出错时将抛出错误. */
int
luaseri_unpack(lua_State *L) {
	if (lua_isnoneornil(L,1)) {
		return 0;
	}
	void * buffer;
	int len;
	if (lua_type(L,1) == LUA_TSTRING) {
		size_t sz;
		buffer = (void *)lua_tolstring(L,1,&sz);
		len = (int)sz;
	} else {
		buffer = lua_touserdata(L,1);
		len = luaL_checkinteger(L,2);
	}
	if (len == 0) {
		return 0;
	}
	if (buffer == NULL) {
		return luaL_error(L, "deserialize null pointer");
	}

	lua_settop(L,1);
	struct read_block rb;
	rball_init(&rb, buffer, len);

	int i;
	for (i=0;;i++) {
		/* 每当压入 8 值的时候都需要确保栈有足够的空闲位置 */
		if (i%8==7) {
			luaL_checkstack(L,LUA_MINSTACK,NULL);
		}
		uint8_t type = 0;
		uint8_t *t = rb_read(&rb, sizeof(type));
		if (t==NULL)
			break;
		type = *t;
		push_value(L, &rb, type & 0x7, type>>3);
	}

	// Need not free buffer

	return lua_gettop(L) - 1;
}

/* [lua_api] 将 Lua 虚拟机栈上的数据序列化成二进制数据, 并将打包成功后的二进制数据内存块指针压栈成 lightuserdata,
 * 以及长度压栈返回. 如果参数的类型不正确, 或者表嵌套过深将向 Lua 抛出错误.
 *
 * 参数: 数据类型必须是 nil, number, boolean, string, lightuserdata 和 table 中的一种, 否则将抛出错误.
 *       如果参数是表, 它嵌套的子表不能超过 MAX_DEPTH 层.
 *
 * 返回: 序列化后的二进制数据和数据长度 */
int
luaseri_pack(lua_State *L) {
	struct block temp;
	temp.next = NULL;
	struct write_block wb;
	wb_init(&wb, &temp);
	pack_from(L,&wb,0);
	assert(wb.head == &temp);
	seri(L, &temp, wb.len);

	wb_free(&wb);

	return 2;
}
