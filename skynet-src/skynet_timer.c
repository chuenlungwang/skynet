#include "skynet.h"

#include "skynet_timer.h"
#include "skynet_mq.h"
#include "skynet_server.h"
#include "skynet_handle.h"
#include "spinlock.h"

#include <time.h>
#include <assert.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>

#if defined(__APPLE__)
#include <sys/time.h>
#endif

typedef void (*timer_execute_func)(void *ud,void *arg);

/* 最近触发列表集占据最低 8 位, 层级触发列表集每级占据 6 位. */
#define TIME_NEAR_SHIFT 8
#define TIME_NEAR (1 << TIME_NEAR_SHIFT)
#define TIME_LEVEL_SHIFT 6
#define TIME_LEVEL (1 << TIME_LEVEL_SHIFT)
#define TIME_NEAR_MASK (TIME_NEAR-1)
#define TIME_LEVEL_MASK (TIME_LEVEL-1)

/* 待触发的定时器事件, 将被放在 struct timer_node 毗邻的后面 */
struct timer_event {
	uint32_t handle;    /* 定时器通知的服务句柄 */
	int session;        /* 定时器通知的服务中的唯一消息标识 */
};

/* 定时触发节点结构体, 在结构体毗邻处紧接着定时事件结构 */
struct timer_node {
	struct timer_node *next;     /* 处于列表中下一个触发节点指针, 如果没有时为 NULL */
	uint32_t expire;             /* 触发时间, 单位是厘秒, 如果距离系统启动超过 0xffffffff (497 天)会发生回绕 */
};

/* 定时触发列表结构体 */
struct link_list {
	struct timer_node head;      /* 头结点是哑节点, 真正的头节点是 head.next */
	struct timer_node *tail;     /* 定时触发的尾部指针, 初始化是为 head 地址 */
};

/* 定时器管理器结构. near 为最近的触发列表集, 触发时间在距离现在 0 ~ 0xFF 厘秒之间, 每个节点相距 1 厘秒, 共 0x100 个节点
 * t 为 4 级依次变远的触发列表集, 第一级列表集的触发时间在 0x100 ~ 0x3FFF 厘秒之间, 每个节点相距 0x100 厘秒, 共 0x40 个节点.
 * 第二级列表集的触发时间在距离现在 0x4000 ~ 0xFFFFF 厘秒之间, 每个节点相距 0x4000 厘秒, 共 0x40 个节点.
 * 第三级列表集的触发时间在距离现在 0x100000 ~ 0x3FFFFFF 厘秒之间, 每个节点相距 0x100000 厘秒, 共 0x40 个节点.
 * 第四级列表集的触发时间在距离现在 0x4000000 ~ 0xFFFFFFFF 厘秒之间, 每个节点相距 0x4000000 厘秒, 共 0x40 个节点.
 * 有一点需要注意, 层级列表集的首节点是不会使用的, 除了当时间距离回绕时, 回绕的定时器会从第四级列表集的第一个节点开始放置.
 * 具体需要参考 add_node 函数. */
struct timer {
	struct link_list near[TIME_NEAR];    /* 最近的触发列表集 */
	struct link_list t[4][TIME_LEVEL];   /* 依次变远的层级触发列表集 */
	struct spinlock lock;      /* 当前定时器的自旋锁, 在多线程情况下注册定时事件与触发定时事件是需要加锁 */
	uint32_t time;             /* 当前时间, 单位厘秒, 是触发定时事件的依据, 与 current 的区别在于 time 的初始值是 0,
	                              而 current 的初始值与墙上时钟有关, time 每次只增加 1 厘秒, 并且伴随着定时事件触发,
	                              具体参见 timer_shift 函数 */
	uint32_t starttime;        /* 系统启动时间点, 单位秒 */
	uint64_t current;          /* 当前时间, 单位厘秒, 与 starttime 一起构成了墙上时钟 */
	uint64_t current_point;    /* 当前时间的精确时间戳, 单位厘秒, 不会回绕, 用于计算系统运行时间 */
};

static struct timer * TI = NULL;

/* 返回并清空整个定时触发列表的内联函数, 此函数使头部哑节点的下一个节点为 NULL,
 * 将尾部指针指向头部哑节点, 并返回头部哑节点之后的第一个节点的指针,
 * 此指针在列表为空的情况下将返回 NULL. 此函数是非线程安全的, 多线程使用时需要加锁. */
static inline struct timer_node *
link_clear(struct link_list *list) {
	struct timer_node * ret = list->head.next;
	list->head.next = 0;
	list->tail = &(list->head);

	return ret;
}

/* 将一个定时触发节点添加到定时触发列表的尾部. 此函数是非线程安全的. */
static inline void
link(struct link_list *list,struct timer_node *node) {
	list->tail->next = node;
	list->tail = node;
	node->next=0;
}

/* 根据触发节点的触发时间距离现在的远近, 将其添加到定时器管理器的最近触发列表集或者层级触发列表集的某条触发列表中.
 * 参数 T 为定时器管理器, node 为触发节点.
 * 
 * 具体是哪条触发列表取决于触发时间距离现在的时间的落在哪条列表的区间范围内, 可参考 struct timer 定义了解.
 * 如果触发时间在 0 ~ 0xFF 厘秒之间添加到最近触发列表集的某条触发列表中. 如果在 0x100 ~ 0x3FFF 厘秒之间
 * 添加到第一级触发列表集的某条触发列表中. 如果在 0x4000 ~ 0xFFFFF 厘秒之间添加到第二级触发列表集
 * 的某条触发列表中. 如果在 0x100000 ~ 0x3FFFFFF 厘秒之间添加到第三级触发列表集的某条触发列表中.
 * 如果在 0x4000000 ~ 0xFFFFFFFF 厘秒之间添加到第四级触发列表集中的某条触发队列中.
 * 
 * 假设触发时间距离现在是 0x378 , 此时间距离的最高有效位落在第一级的触发列表集区间, 且在此区间刚好为 0x3 , 因而,
 * 将落在第一级触发列表集的第四个节点处(索引是 3)的触发列表.
 * 
 * 此处需要注意的一点是层级列表集的首节点是不会使用的, 除了触发节点的触发时间产生回绕时,
 * 将从第四级列表集的第一个节点处的列表开始放置. 此函数是非线程安全的. */
static void
add_node(struct timer *T,struct timer_node *node) {
	uint32_t time=node->expire;
	uint32_t current_time=T->time;
	
	/* 如果触发时间和当前时间仅仅在最近触发列表集区间内有差异, 说明触发距离的最高有效位落在此区间,
	   那么将被添加到最近触发列表集. 否则寻找差异最高有效位落在哪个列表集的区间,
	   并根据在那个区间的值作为索引添加进去. 当时间回绕时, 回绕的定时器会从第四级列表集的第一个
	   节点开始放置, 因为所有区间都是有差异的, 但最高区间的值却是从 0 开始, 而非回绕都至少是 1 . */

	if ((time|TIME_NEAR_MASK)==(current_time|TIME_NEAR_MASK)) {
		link(&T->near[time&TIME_NEAR_MASK],node);
	} else {
		int i;
		uint32_t mask=TIME_NEAR << TIME_LEVEL_SHIFT;
		for (i=0;i<3;i++) {
			if ((time|(mask-1))==(current_time|(mask-1))) {
				break;
			}
			mask <<= TIME_LEVEL_SHIFT;
		}

		link(&T->t[i][((time>>(TIME_NEAR_SHIFT + i*TIME_LEVEL_SHIFT)) & TIME_LEVEL_MASK)],node);	
	}
}

/* 分配内存并构造一个定时触发节点, 并添加到定时器管理器中, 添加算法参见 add_node 函数.
 * 参数 T 为定时器管理器, arg 为定时器事件, sz 为定时器事件结构的大小, time 为触发时间距离现在的距离.
 * 此函数是线程安全的. */
static void
timer_add(struct timer *T,void *arg,size_t sz,int time) {
	struct timer_node *node = (struct timer_node *)skynet_malloc(sizeof(*node)+sz);
	memcpy(node+1,arg,sz);

	SPIN_LOCK(T);

	/* 读取管理器中的当前时间和操作触发列表集都是非线程安全的, 因而需要在同步块中执行. */
	node->expire=time+T->time;
	add_node(T,node);

	SPIN_UNLOCK(T);
}

/* 将层级列表集中的某个触发列表移动根据触发时间距今长短移动到较低层次的列表集
 * 或者最近触发列表集的某个触发列表中. 方法是先移除列表再添加回列表集中, 添加算法参见 add_node 函数.
 * 参数 T 是定时器管理器, level 是层级列表集的索引, idx 是触发列表在此层级列表集中的索引.
 * 此函数是非线程安全的. */
static void
move_list(struct timer *T, int level, int idx) {
	struct timer_node *current = link_clear(&T->t[level][idx]);
	while (current) {
		struct timer_node *temp=current->next;
		add_node(T,current);
		current=temp;
	}
}

/* 对当前时间增加 1 厘秒, 并判断当前时间是否到了需要重新安排触发列表的时候执行相应的安排.
 * 具体说就是到达 0x100 的整数倍时对第一级触发列表集中的触发列表进行重新安排,
 * 0x4000 的整数倍则是第二级触发列表集中的触发列表, 0x100000 的整数倍是第三级, 0x4000000 的整数倍是第四级.
 * 具体是该集中哪个列表取决于所在区间的值, 这个值将作为索引. 重新安排的算法参考 add_node 函数.
 *
 * 需要注意的是层级列表集的首节点是不会使用的, 除了当时间距离回绕时, 回绕的定时器会从第四级列表集的第一个节点开始放置,
 * 因而, 如果当前时间回绕时需要在那里重新安排触发列表. 此函数不是线程安全的. */
static void
timer_shift(struct timer *T) {
	int mask = TIME_NEAR;
	uint32_t ct = ++T->time;
	if (ct == 0) {
		/* 如果当前时间产生回绕, 需要重新安排第四级列表集的第一个节点处的列表集 */
		move_list(T, 3, 0);
	} else {
		/* 检查当前时间是哪个级别时间范围的整数倍, 并以上一级区间的值为索引进行重新安排.
		 * 如果不是任何级别时间范围的整数倍, 将不做任何重新安排. */
		uint32_t time = ct >> TIME_NEAR_SHIFT;
		int i=0;

		while ((ct & (mask-1))==0) {
			int idx=time & TIME_LEVEL_MASK;
			/* idx!=0 正表明层级列表集的首节点不会使用 */
			if (idx!=0) {
				move_list(T, i, idx);
				break;				
			}
			mask <<= TIME_LEVEL_SHIFT;
			time >>= TIME_LEVEL_SHIFT;
			++i;
		}
	}
}

/* 分发整个定时触发列表中的定时事件, 此函数要求列表中至少有一个定时事件节点.
 * 投递的消息类型为 PTYPE_RESPONSE, session 为注册定时器时传入的值,
 * 到 handle 也为注册定时器时传入的值为地址的服务中去.
 * 此函数同时负责将回收为 struct timer_node 结构分配的内存. */
static inline void
dispatch_list(struct timer_node *current) {
	do {
		struct timer_event * event = (struct timer_event *)(current+1);
		struct skynet_message message;
		message.source = 0;
		message.session = event->session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		skynet_context_push(event->handle, &message);
		
		struct timer_node * temp = current;
		current=current->next;
		skynet_free(temp);	
	} while (current);
}

/* 分发此时到期的定时事件, 此函数会以线程安全的方式不断监测是否有新添加进来的当前时间就过期的定时事件.
 * 如果检查到就一并分发, 如果检查不到就会留至下一次分发时分发. */
static inline void
timer_execute(struct timer *T) {
	int idx = T->time & TIME_NEAR_MASK;
	
	while (T->near[idx].head.next) {
		struct timer_node *current = link_clear(&T->near[idx]);
		SPIN_UNLOCK(T);
		// dispatch_list don't need lock T
		dispatch_list(current);
		SPIN_LOCK(T);
	}
}

/* 更新当前时间并分发所有已经到期的定时事件. 此函数是线程安全的. */
static void 
timer_update(struct timer *T) {
	SPIN_LOCK(T);

	/* 第一件做的事情是分发当前时间未更新的情况下到期的定时事件, 这些事件
	   是在上次分发运行中添加进来而未检查到的. 如果不这样做, 它们将永远丢失. */
	// try to dispatch timeout 0 (rare condition)
	timer_execute(T);

	/* 然后才是更新当前时间, 重新安排层级列表集中的定时触发事件, 并分发此时到期的定时事件 */
	// shift time first, and then dispatch timer message
	timer_shift(T);

	timer_execute(T);

	SPIN_UNLOCK(T);
}

/* 构建定时器对象, 包括分配内存、初始化触发列表集、初始化锁并将当前时间 time 置为 0.
 * 此函数返回初始化好的定时器对象. */
static struct timer *
timer_create_timer() {
	struct timer *r=(struct timer *)skynet_malloc(sizeof(struct timer));
	/* 将 time 初始化为 0 */
	memset(r,0,sizeof(*r));

	int i,j;

	/* 初始化最近的触发列表集 */
	for (i=0;i<TIME_NEAR;i++) {
		link_clear(&r->near[i]);
	}

	/* 初始化 4 级触发时间较远的触发列表集 */
	for (i=0;i<4;i++) {
		for (j=0;j<TIME_LEVEL;j++) {
			link_clear(&r->t[i][j]);
		}
	}

	SPIN_INIT(r)

	/* current 以及除了 time 以外的其它时间字段还会进一步初始化 */
	r->current = 0;

	return r;
}

/* 为一个服务的某次会话注册一个定时器事件. 如果传入的时间小于等于 0 将会立即发送
 * 类型为 PTYPE_RESPONSE 的消息给该服务会话, 否则将以线程安全的方式添加到触发列表集中.
 * 参数 handle 是服务地址, time 是距离现在的触发时间单位是厘秒, session 是服务中的会话.
 * 此函数是线程安全的. */
int
skynet_timeout(uint32_t handle, int time, int session) {
	if (time <= 0) {
		struct skynet_message message;
		message.source = 0;
		message.session = session;
		message.data = NULL;
		message.sz = (size_t)PTYPE_RESPONSE << MESSAGE_TYPE_SHIFT;

		if (skynet_context_push(handle, &message)) {
			return -1;
		}
	} else {
		struct timer_event event;
		event.handle = handle;
		event.session = session;
		timer_add(TI, &event, sizeof(event), time);
	}

	return session;
}

/* 获取操作系统墙上时间, 时间计算为从 1970 年 1 月 1 日 00:00 经过的秒数, 不足一秒的记录为厘秒数.
 * 返回的时间与时区无关, 传入参数 sec 用来接收秒数, cs 用来接收厘秒. */
// centisecond: 1/100 second
static void
systime(uint32_t *sec, uint32_t *cs) {
/* 在 OSX 中没有定义 clock_gettime 函数而只定义了 gettimeofday 函数,
   并且 struct timespec 包含秒和纳秒, 而 struct timeval 包含秒和微秒. */
#if !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_REALTIME, &ti);
	*sec = (uint32_t)ti.tv_sec;
	*cs = (uint32_t)(ti.tv_nsec / 10000000);
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	*sec = tv.tv_sec;
	*cs = tv.tv_usec / 10000;
#endif
}

/* 获取自某个确定时间点的时间戳, 单位是厘秒. 在非 OSX 的环境下获取的是自操作系统启动后的厘秒数,
 * 此时时间是不受用户设置墙上时钟影响的. 由于 OSX 中没有类似函数, 获取的是
 * 自 1970 年 1 月 1 日 00:00 经过的厘秒数, 这个时间是会受到用户设置的影响的.
 * 返回值是距离某个确定时间点的厘秒数.
 * 此函数的用途在于通过比较两个时间戳的差值获取一个精确的时间流逝. */
static uint64_t
gettime() {
	uint64_t t;
#if !defined(__APPLE__)
	struct timespec ti;
	clock_gettime(CLOCK_MONOTONIC, &ti);
	t = (uint64_t)ti.tv_sec * 100;
	t += ti.tv_nsec / 10000000;
#else
	struct timeval tv;
	gettimeofday(&tv, NULL);
	t = (uint64_t)tv.tv_sec * 100;
	t += tv.tv_usec / 10000;
#endif
	return t;
}

/* 更新定时器中的时间并触发定时器. 若时间变化将先增加至 current 厘秒字段, 仅当其无法表示时才更新至 starttime 秒字段中.
 * 同时更新的还有表示当前时间点的 current_point 字段. */
void
skynet_updatetime(void) {
	/* 获取到精确的距离某个时间点的时间戳, 用于确定时间流逝. */
	uint64_t cp = gettime();
	if(cp < TI->current_point) {
		skynet_error(NULL, "time diff error: change from %lld to %lld", cp, TI->current_point);
		TI->current_point = cp;
	} else if (cp != TI->current_point) {
		uint32_t diff = (uint32_t)(cp - TI->current_point);
		TI->current_point = cp;
		TI->current += diff;
		int i;
		for (i=0;i<diff;i++) {
			timer_update(TI);
		}
	}
}

/* 获取启动时间, 时间计算为从 1970 年 1 月 1 日 00:00 经过的秒数. */
uint32_t
skynet_starttime(void) {
	return TI->starttime;
}

/* 获取当前时间, 单位是厘秒, 与 skynet_starttime 之和构成墙上时钟. */
uint64_t 
skynet_now(void) {
	return TI->current;
}

/* 初始化定时器模块, 初始工作包括构建定时器对象, 初始化时间系统的当前时间、启动时间、
 * 启动时间戳和当前时间戳. */
void 
skynet_timer_init(void) {
	TI = timer_create_timer();
	uint32_t current = 0;
	systime(&TI->starttime, &current);
	TI->current = current;
	TI->current_point = gettime();
}

