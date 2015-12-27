#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <lua.h>
#include <stdio.h>

#include "malloc_hook.h"
#include "skynet.h"
#include "atomic.h"

static size_t _used_memory = 0;     /* 记录分配的内存大小 */
static size_t _memory_block = 0;    /* 记录分配的内存块数 */

/* 记录每个服务占用内存的结构 */
typedef struct _mem_data {
	uint32_t handle;        /* 服务地址 */
	ssize_t allocated;      /* 占用的内存大小 */
} mem_data;

#define SLOT_SIZE 0x10000
/* PREFIX_SIZE 是使用 jemalloc 内存分配器时跟踪内存分配时放在堆内存的末端的服务地址的大小 */
#define PREFIX_SIZE sizeof(uint32_t)

static mem_data mem_stats[SLOT_SIZE];


#ifndef NOUSE_JEMALLOC

#include "jemalloc.h"

// for skynet_lalloc use
#define raw_realloc je_realloc
#define raw_free je_free

/* 获取服务器地址 handle 所对应的分配内存记录地址. 如果服务所对应的槽被其它服务占据将返回 0,
 * 如果槽原本没有被服务占据或者槽中分配的内存大小记录值小于等于 0 , 槽将被此服务占据, 并且
 * 小于 0 的内存大小将被置 0 .
 * 
 * 参数: handle 为服务地址
 * 返回: 此服务所分配的内存记录地址, 或者返回 0 表示没有获取到分配内存记录地址 . */
static ssize_t*
get_allocated_field(uint32_t handle) {
	int h = (int)(handle & (SLOT_SIZE - 1));
	mem_data *data = &mem_stats[h];
	uint32_t old_handle = data->handle;
	ssize_t old_alloc = data->allocated;
	/* 两种情况下可以占据此槽, 之前此槽没有被占据, 由于释放内存, 内存记录被减到 0 及其以下. */
	if(old_handle == 0 || old_alloc <= 0) {
		// data->allocated may less than zero, because it may not count at start.
		if(!ATOM_CAS(&data->handle, old_handle, handle)) {
			return 0;
		}
		if (old_alloc < 0) {
			ATOM_CAS(&data->allocated, old_alloc, 0);
		}
	}
	if(data->handle != handle) {
		return 0;
	}
	return &data->allocated;
}

/* 向内存记录钩子中增加服务地址为 handle 的服务新分配的 __n 内存大小.
 * 参数: handle 为服务地址, __n 为本次分配的内存大小
 * 函数无返回值 */
inline static void 
update_xmalloc_stat_alloc(uint32_t handle, size_t __n) {
	ATOM_ADD(&_used_memory, __n);
	ATOM_INC(&_memory_block); 
	ssize_t* allocated = get_allocated_field(handle);
	if(allocated) {
		ATOM_ADD(allocated, __n);
	}
}

/* 向内存记录钩子中减小服务地址为 handle 的服务刚释放的 __n 内存大小.
 * 参数: handle 为服务地址, __n 为本次分配的内存大小
 * 函数无返回值 */
inline static void
update_xmalloc_stat_free(uint32_t handle, size_t __n) {
	ATOM_SUB(&_used_memory, __n);
	ATOM_DEC(&_memory_block);
	ssize_t* allocated = get_allocated_field(handle);
	if(allocated) {
		ATOM_SUB(allocated, __n);
	}
}

/* 在分配内存后填充服务地址, 填充的位置是分配内存的末尾, 并在内存记录钩子中增加分配的内存.
 * 参数: ptr 是分配内存的起始地址
 * 返回: 函数返回填充完后缀的内存起始指针. */
inline static void*
fill_prefix(char* ptr) {
	uint32_t handle = skynet_current_handle();
	size_t size = je_malloc_usable_size(ptr);
	uint32_t *p = (uint32_t *)(ptr + size - sizeof(uint32_t));
	memcpy(p, &handle, sizeof(handle));

	update_xmalloc_stat_alloc(handle, size);
	return ptr;
}

/* 在销毁内存前清理后缀, 并在内存记录钩子中减少待销毁的内存.
 * 参数: ptr 是待销毁内存的起始地址
 * 返回: 函数返回清理完后缀的内存起始地址. */
inline static void*
clean_prefix(char* ptr) {
	size_t size = je_malloc_usable_size(ptr);
	uint32_t *p = (uint32_t *)(ptr + size - sizeof(uint32_t));
	uint32_t handle;
	memcpy(&handle, p, sizeof(handle));
	update_xmalloc_stat_free(handle, size);
	return ptr;
}

/* 当无法继续分配内存时, 写入错误消息, 并退出进程. */
static void malloc_oom(size_t size) {
	fprintf(stderr, "xmalloc: Out of memory trying to allocate %zu bytes\n",
		size);
	fflush(stderr);
	abort();
}

/* 以人类可读的方式向标准误 stderr 中输出当前的 jemalloc 统计信息. */
void 
memory_info_dump(void) {
	je_malloc_stats_print(0,0,0);
}

/* 读取 jemalloc 内存分配器的设置键 name 对应的值或者当 newval 指针不为 NULL 时, 为它设置新的值.
 * 注意此函数中 newval 指向的是一个类型为 size_t 的值.
 *
 * 参数: name 是以点号分隔的 jemalloc 设置键, 参见 jemalloc 的文档, newval 是设置的新值, 当仅读取时传入 NULL.
 * 返回: 原先的值, 类型为 size_t. */
size_t 
mallctl_int64(const char* name, size_t* newval) {
	size_t v = 0;
	size_t len = sizeof(v);
	if(newval) {
		je_mallctl(name, &v, &len, newval, sizeof(size_t));
	} else {
		je_mallctl(name, &v, &len, NULL, 0);
	}
	// skynet_error(NULL, "name: %s, value: %zd\n", name, v);
	return v;
}

/* 读取 jemalloc 内存分配器的设置键 name 对应的值或者当 newval 指针不为 NULL 时, 为它设置新的值.
 * 注意此函数中 newval 指向的是一个类型为 int 的值.
 *
 * 参数: name 是以点号分隔的 jemalloc 设置键, 参见 jemalloc 的文档, newval 是设置的新值, 当仅读取时传入 NULL.
 * 返回: 原先的值, 类型为 int. */
int 
mallctl_opt(const char* name, int* newval) {
	int v = 0;
	size_t len = sizeof(v);
	if(newval) {
		int ret = je_mallctl(name, &v, &len, newval, sizeof(int));
		if(ret == 0) {
			skynet_error(NULL, "set new value(%d) for (%s) succeed\n", *newval, name);
		} else {
			skynet_error(NULL, "set new value(%d) for (%s) failed: error -> %d\n", *newval, name, ret);
		}
	} else {
		je_mallctl(name, &v, &len, NULL, 0);
	}

	return v;
}

// hook : malloc, realloc, free, calloc

/* 分配大小为 size 的内存块, 并返回起始指针. 函数使用 jemalloc 内存分配器, 并记录分配的内存大小.
 * 如果无法分配更多内存将会导致进程退出.
 *
 * 参数: 待分配的大小 size .
 * 返回: 分配好的内存的起始地址. */
void *
skynet_malloc(size_t size) {
	void* ptr = je_malloc(size + PREFIX_SIZE);
	if(!ptr) malloc_oom(size);
	return fill_prefix(ptr);
}

/* 对 ptr 指向的内存块进行重新分配, 分配后的内存块中将尽可能多的保存原有的内容. 函数返回重新分配后的
 * 内存起始地址. 如果 ptr 是 NULL 则仅仅是分配内存. 尽量不要使得 size 为 0, 这种情况可以调用 skynet_free 函数.
 * 当没有更多的内存可以分配时, 此函数会导致进程退出. 此函数功能与标准库中的 realloc 完全一致, 除了会加
 * 上更多的内存记录来跟踪内存分配.
 *
 * 参数: ptr 是重新分配内存前的起始指针, size 是重新分配的内存大小
 * 返回: 重新分配的内存起始指针 */
void *
skynet_realloc(void *ptr, size_t size) {
	if (ptr == NULL) return skynet_malloc(size);

	void* rawptr = clean_prefix(ptr);
	void *newptr = je_realloc(rawptr, size+PREFIX_SIZE);
	if(!newptr) malloc_oom(size);
	return fill_prefix(newptr);
}

/* 释放指针 ptr 指针的堆内存块. */
void
skynet_free(void *ptr) {
	if (ptr == NULL) return;
	void* rawptr = clean_prefix(ptr);
	je_free(rawptr);
}

/* 分配长度为 nmemb*size 的内存块, 并将其内容置为 0. 当没有更多的内存可以分配时, 此函数会导致进程退出.
 * 此函数与标准库中的 calloc 的功能完全一样, 此函数使用 jemalloc 内存分配器, 并且一样跟踪了内存分配记录.
 *
 * 参数: nmemb 为分配内存数组的个数, size 为每个元素的大小
 * 返回: 分配好的内存的起始地址 */
void *
skynet_calloc(size_t nmemb,size_t size) {
	void* ptr = je_calloc(nmemb + ((PREFIX_SIZE+size-1)/size), size );
	if(!ptr) malloc_oom(size);
	return fill_prefix(ptr);
}

#else

// for skynet_lalloc use
#define raw_realloc realloc
#define raw_free free

/* 使用标准库中的内存分配函数无法输出内存分配统计信息. */
void 
memory_info_dump(void) {
	skynet_error(NULL, "No jemalloc");
}

/* 此函数仅在使用 jemalloc 的情况下可用. */
size_t 
mallctl_int64(const char* name, size_t* newval) {
	skynet_error(NULL, "No jemalloc : mallctl_int64 %s.", name);
	return 0;
}

/* 此函数仅在使用 jemalloc 的情况下可用. */
int 
mallctl_opt(const char* name, int* newval) {
	skynet_error(NULL, "No jemalloc : mallctl_opt %s.", name);
	return 0;
}

#endif

/* 查询到目前为止分配的所有内存的大小. */
size_t
malloc_used_memory(void) {
	return _used_memory;
}

/* 查询到目前为止分配的内存块的数量. */
size_t
malloc_memory_block(void) {
	return _memory_block;
}

/* skynet 的日志中打印所有服务占用的内存大小, 并最终输出总的内存占用大小. */
void
dump_c_mem() {
	int i;
	size_t total = 0;
	skynet_error(NULL, "dump all service mem:");
	for(i=0; i<SLOT_SIZE; i++) {
		mem_data* data = &mem_stats[i];
		/* [bugfix]data->allocated > 0 更为妥当[/bugfix] */
		if(data->handle != 0 && data->allocated != 0) {
			total += data->allocated;
			skynet_error(NULL, "0x%x -> %zdkb", data->handle, data->allocated >> 10);
		}
	}
	skynet_error(NULL, "+total: %zdkb",total >> 10);
}

/* 将字符串 str 的内容复制到堆内存中, 并返回复制后的字符串的起始地址.
 * 参数: str 是待复制的字符串
 * 返回: 复制后的字符串的起始地址 */
char *
skynet_strdup(const char *str) {
	size_t sz = strlen(str);
	char * ret = skynet_malloc(sz+1);
	memcpy(ret, str, sz+1);
	return ret;
}

/* lua 的内存分配函数. 函数功能如 lua 文档对 lua_Alloc 描述一致. 当 nsize 为 0 时将回收原来的内存块并返回 NULL.
 * 其它情况将释放原来的内存, 并重新分配内存, 整个过程将最大限度保证内容不变. 此函数分配的内存不会被跟踪记录.
 *
 * 参数: ud 是 lua_newstate 传过来的指针, ptr 是已分配即将被回收的内存, osize 是原来的内存尺寸, nsize 是重新分配后的内存尺寸
 * 返回: 重新分配后的内存的起始地址, 如果 nsize 为 0 则返回 NULL, 如果无法完成内存分配工作也将返回 NULL . */
void * 
skynet_lalloc(void *ud, void *ptr, size_t osize, size_t nsize) {
	if (nsize == 0) {
		raw_free(ptr);
		return NULL;
	} else {
		return raw_realloc(ptr, nsize);
	}
}

/* 将 skynet 中所有服务占用的内存以 table 的形式传递给 lua 虚拟机 L . table 中的键为服务句柄, 值为占用的内存大小.
 * 参数: L 为 lua 虚拟机
 * 返回: 1 表示 lua 虚拟机值栈栈顶上的 table 为其唯一返回值. */
int
dump_mem_lua(lua_State *L) {
	int i;
	lua_newtable(L);
	for(i=0; i<SLOT_SIZE; i++) {
		mem_data* data = &mem_stats[i];
		/* [bugfix]data->allocated > 0 更为妥当[/bugfix] */
		if(data->handle != 0 && data->allocated != 0) {
			lua_pushinteger(L, data->allocated);
			lua_rawseti(L, -2, (lua_Integer)data->handle);
		}
	}
	return 1;
}

size_t
malloc_current_memory(void) {
	uint32_t handle = skynet_current_handle();
	int i;
	for(i=0; i<SLOT_SIZE; i++) {
		mem_data* data = &mem_stats[i];
		if(data->handle == (uint32_t)handle && data->allocated != 0) {
			return (size_t) data->allocated;
		}
	}
	return 0;
}

void
skynet_debug_memory(const char *info) {
	// for debug use
	uint32_t handle = skynet_current_handle();
	size_t mem = malloc_current_memory();
	fprintf(stderr, "[:%08x] %s %p\n", handle, info, (void *)mem);
}
