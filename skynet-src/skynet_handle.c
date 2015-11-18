#include "skynet.h"

#include "skynet_handle.h"
#include "skynet_server.h"
#include "rwlock.h"

#include <stdlib.h>
#include <assert.h>
#include <string.h>

/* 服务插槽的起始容量 */
#define DEFAULT_SLOT_SIZE 4
/* 名字数组的最大容量 */
#define MAX_SLOT_SIZE 0x40000000

/* skynet 服务的名字结构, 在调用时用 .name 表示服务名字 */
struct handle_name {
	char * name;        /* 服务的名字 */
	uint32_t handle;    /* 服务的地址 */
};

/* 所有服务的句柄存储器, 服务以其地址的哈希值为索引存储于插槽中, 而服务名字则以
 * 字典顺序存储. */
struct handle_storage {
	struct rwlock lock;                 /* 读写锁, 当需要以地址获取服务对象时对读锁加锁, 当插入新的服务时对写锁加锁 */

	uint32_t harbor;                    /* 当前 skynet 节点的 harbor id , 但是被移位到了最高 8 位 */
	uint32_t handle_index;              /* 下一个服务地址分配的起点, 此值保证单调递增 */
	int slot_size;                      /* 容纳服务指针的插槽的容量, 为 2 的倍数, 方便做位与形式的哈希运算 */
	struct skynet_context ** slot;      /* 容纳服务指针的插槽 */
	
	int name_cap;                       /* 供存储名字的内存可容纳的名字个数 */
	int name_count;                     /* 当前已经存储的名字个数 */
	struct handle_name *name;           /* 容纳服务名字的数组 */
};

static struct handle_storage *H = NULL;

/* 将服务 ctx 注册到服务句柄存储中去并返回相应的服务地址.
 * 参数: ctx 为待注册的服务
 * 返回: 注册成功后返回服务的地址 */
uint32_t
skynet_handle_register(struct skynet_context *ctx) {
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);
	
	for (;;) {
		/* 以 handle_index 为起点查询插槽中所有的空槽, 查询的方式是以增加后的 handle 值
		 * 对槽的大小位与并作为其索引查询是否空闲. 整个过程中 handle 和 handle_index 都是单调递增的.
		 * 如果没有一个空闲槽, 则对插槽进行扩容再插入. */
		int i;
		for (i=0;i<s->slot_size;i++) {
			/* 将值与插槽容量的位与值作为哈希值 */
			uint32_t handle = (i+s->handle_index) & HANDLE_MASK;
			int hash = handle & (s->slot_size-1);
			if (s->slot[hash] == NULL) {
				s->slot[hash] = ctx;
				s->handle_index = handle + 1;

				rwlock_wunlock(&s->lock);

				handle |= s->harbor;
				return handle;
			}
		}
		assert((s->slot_size*2 - 1) <= HANDLE_MASK);
		struct skynet_context ** new_slot = skynet_malloc(s->slot_size * 2 * sizeof(struct skynet_context *));
		memset(new_slot, 0, s->slot_size * 2 * sizeof(struct skynet_context *));
		
		/* 扩容之后需要将元素重新哈希到新的位置去 */
		for (i=0;i<s->slot_size;i++) {
			int hash = skynet_context_handle(s->slot[i]) & (s->slot_size * 2 - 1);
			assert(new_slot[hash] == NULL);
			new_slot[hash] = s->slot[i];
		}
		skynet_free(s->slot);
		s->slot = new_slot;
		s->slot_size *= 2;
	}
}

/* 将一个地址为 handle 的服务卸载, 并将与之相关的名字删除. 当卸载成功时返回 1 , 如果尝试卸载时服务
 * 已经不存在了, 那么不做任何操作并返回 0 . 整个函数是线程安全的.
 *
 * 参数: handle 是待卸载的服务的地址
 * 返回: 返回 1 指示卸载成功, 0 指示服务不存在. */
int
skynet_handle_retire(uint32_t handle) {
	int ret = 0;
	struct handle_storage *s = H;

	rwlock_wlock(&s->lock);

	/* 将服务地址与插槽容量的位与值作为哈希值 */
	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];

	/* 查询到服务存在且地址确实是传入的参数时才会执行卸载 */
	if (ctx != NULL && skynet_context_handle(ctx) == handle) {
		s->slot[hash] = NULL;
		ret = 1;
		/* 将服务的名字删除, 且空出来的位置被后面的名字填充掉. */
		int i;
		int j=0, n=s->name_count;
		for (i=0; i<n; ++i) {
			if (s->name[i].handle == handle) {
				skynet_free(s->name[i].name);
				continue;
			} else if (i!=j) {
				s->name[j] = s->name[i];
			}
			++j;
		}
		s->name_count = j;
	} else {
		ctx = NULL;
	}

	rwlock_wunlock(&s->lock);

	if (ctx) {
		// release ctx may call skynet_handle_* , so wunlock first.
		skynet_context_release(ctx);
	}

	return ret;
}

/* 将整个 skynet 进程中的服务全部卸载. */
void 
skynet_handle_retireall() {
	struct handle_storage *s = H;
	for (;;) {
		int n=0;
		int i;
		/* 遍历整个插槽并卸载其中的服务, 计数器 n 表示卸载成功的服务数 */
		for (i=0;i<s->slot_size;i++) {
			rwlock_rlock(&s->lock);
			struct skynet_context * ctx = s->slot[i];
			uint32_t handle = 0;
			if (ctx)
				handle = skynet_context_handle(ctx);
			rwlock_runlock(&s->lock);
			if (handle != 0) {
				if (skynet_handle_retire(handle)) {
					++n;
				}
			}
		}
		/* 循环直到所有的服务都被卸载了 */
		if (n==0)
			return;
	}
}

/* 由服务地址获取到服务对象, 并对服务的引用计数加 1 . 如果存在相应的服务则返回服务对象,
 * 否则将返回 NULL.
 *
 * 参数: handle 是服务地址
 * 返回: 查找到并引用计数加 1 的服务或者 NULL */
struct skynet_context * 
skynet_handle_grab(uint32_t handle) {
	struct handle_storage *s = H;
	struct skynet_context * result = NULL;

	rwlock_rlock(&s->lock);

	/* 将服务地址与插槽容量的位与值作为哈希值 */
	uint32_t hash = handle & (s->slot_size-1);
	struct skynet_context * ctx = s->slot[hash];
	if (ctx && skynet_context_handle(ctx) == handle) {
		result = ctx;
		skynet_context_grab(result);
	}

	rwlock_runlock(&s->lock);

	return result;
}

/* 在服务句柄存储中查找名字为 name 的服务的地址. 如果查到则返回服务地址, 查不到则返回 0 .
 * 此函数是线程安全的.
 *
 * 参数: name 是待查找的服务的名字
 * 返回: 查到时返回服务地址, 未查到时返回 0 . */
uint32_t 
skynet_handle_findname(const char * name) {
	struct handle_storage *s = H;

	rwlock_rlock(&s->lock);

	uint32_t handle = 0;

	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			handle = n->handle;
			break;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}

	rwlock_runlock(&s->lock);

	return handle;
}

/* 将服务名字 name 以及相关联的服务地址 handle 插入到服务句柄存储的名字数组中索引为 before 的位置,
 * 并将其后的所有名字后移. 以此得到一个按字典排序的名字数组. 如果名字数组的容量不够时, 将扩展数组.
 *
 * 参数: s 是服务句柄存储, name 是待插入的名字, handle 是与名字相关联的服务地址, before 是待插入的索引
 * 此函数无返回值 */
static void
_insert_name_before(struct handle_storage *s, char *name, uint32_t handle, int before) {
	if (s->name_count >= s->name_cap) {
		s->name_cap *= 2;
		assert(s->name_cap <= MAX_SLOT_SIZE);
		struct handle_name * n = skynet_malloc(s->name_cap * sizeof(struct handle_name));
		/* 将名字放到新的数组中去, 索引 before 的元素会被腾出来, 其后的元素会被后移. */
		int i;
		for (i=0;i<before;i++) {
			n[i] = s->name[i];
		}
		for (i=before;i<s->name_count;i++) {
			n[i+1] = s->name[i];
		}
		skynet_free(s->name);
		s->name = n;
	} else {
		/* 直接后移其后的元素, 使得索引为 before 的元素被腾出来 */
		int i;
		for (i=s->name_count;i>before;i--) {
			s->name[i] = s->name[i-1];
		}
	}
	s->name[before].name = name;
	s->name[before].handle = handle;
	s->name_count ++;
}

/* 将名字 name 及其相关联的服务地址 handle 插入到服务句柄存储的名字数组中去. 函数首先
 * 会查找名字是否已经存在于名字数组中, 如果是则返回 NULL, 否则执行插入并返回名字数组中的名字指针.
 * 由于名字数组中的名字在堆内存中, 因而返回的名字指针可以传递到任意地方.
 *
 * 参数: s 是服务句柄存储, name 是待插入的名字, handle 是与名字相关联的服务地址
 * 返回: 服务的名字指针或者名字已经存在时返回 NULL */
static const char *
_insert_name(struct handle_storage *s, const char * name, uint32_t handle) {
	int begin = 0;
	int end = s->name_count - 1;
	while (begin<=end) {
		int mid = (begin+end)/2;
		struct handle_name *n = &s->name[mid];
		int c = strcmp(n->name, name);
		if (c==0) {
			return NULL;
		}
		if (c<0) {
			begin = mid + 1;
		} else {
			end = mid - 1;
		}
	}
	/* 名字被复制到堆内存中 */
	char * result = skynet_strdup(name);

	_insert_name_before(s, result, handle, begin);

	return result;
}

/* 将服务地址 handle 所表示的服务命名为 name. 此函数是线程安全的.
 * 参数: handle 为服务地址, *name 为服务名字, 没有大小限制, 也不要求是堆内存.
 * 返回: 被存储的服务名字, 如果已经存在此名字将返回 NULL. 由于名字存在于堆内存中, 因而可以传递到任意地方. */
const char * 
skynet_handle_namehandle(uint32_t handle, const char *name) {
	rwlock_wlock(&H->lock);

	const char * ret = _insert_name(H, name, handle);

	rwlock_wunlock(&H->lock);

	return ret;
}

/* 对服务句柄模块进行初始化, 分配服务插槽和名字数组, 初始化读写锁以及将 harbor 值移动到最高 8 位.
 * 给句柄存储器分配的内存最终不会被回收, 而是随着进程结束而回收.
 *
 * 参数: harbor 为当前节点的 id
 * 此函数无返回值. */
void 
skynet_handle_init(int harbor) {
	assert(H==NULL);
	struct handle_storage * s = skynet_malloc(sizeof(*H));
	s->slot_size = DEFAULT_SLOT_SIZE;
	s->slot = skynet_malloc(s->slot_size * sizeof(struct skynet_context *));
	memset(s->slot, 0, s->slot_size * sizeof(struct skynet_context *));

	rwlock_init(&s->lock);
	// reserve 0 for system
	s->harbor = (uint32_t) (harbor & 0xff) << HANDLE_REMOTE_SHIFT;

	/* 服务地址分配的起点是 1 , 0 被保留到系统内部 */
	s->handle_index = 1;
	s->name_cap = 2;
	s->name_count = 0;
	s->name = skynet_malloc(s->name_cap * sizeof(struct handle_name));

	H = s;

	// Don't need to free H
}

