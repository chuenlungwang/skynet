#include "skynet.h"
#include "skynet_mq.h"
#include "skynet_handle.h"
#include "spinlock.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdbool.h>

/* 默认队列大小为2的倍数, 对长度取模更加高效 */
#define DEFAULT_QUEUE_SIZE 64
#define MAX_GLOBAL_MQ 0x10000

// 0 means mq is not in global mq.
// 1 means mq is in global mq , or the message is dispatching.

#define MQ_IN_GLOBAL 1
#define MQ_OVERLOAD 1024

/* 单个服务拥有的消息队列, 在系统中作为全局队列的元素, 实现为数组环绕表 */
struct message_queue {
	struct spinlock lock;             /* 队列锁, 在多线程下消息入队和出队时同步使用 */
	uint32_t handle;                  /* 当前队列所属的服务的 handle */
	int cap;                          /* 队列的容量, 为分配的数组大小 */
	int head;                         /* 队列的头结点索引, 初始为 0, 可环绕 */
	int tail;                         /* 队列的尾节点索引, 初始为 0, 可环绕 */
	int release;                      /* 是否被标记为需要销毁 */
	int in_global;                    /* 是否在全局队列中 */
	int overload;                     /* 当过载时显示过载量 */
	int overload_threshold;           /* 消息过载阈值 */
	struct skynet_message *queue;     /* 此队列拥有的二级消息队列, 内存是预先分配的 */
	struct message_queue *next;       /* 此队列在全局队列中的下一个元素, 若没有为 NULL */
};

/* 系统的全局队列, 整个系统只有一条, 实现为单向链表 */
struct global_queue {
	struct message_queue *head;       /* 头结点, 初始时为 NULL */
	struct message_queue *tail;       /* 尾节点, 初始时为 NULL */
	struct spinlock lock;             /* 全局队列锁, 当服务的消息队列入列和出列时同步使用 */
};

static struct global_queue *Q = NULL;

/* 将二级队列入列到全局队列中, 此函数是线程安全的 */
void 
skynet_globalmq_push(struct message_queue * queue) {
	struct global_queue *q= Q;

	/* 在加自旋锁的情况下进行入列, 如果原先没有元素, 则头尾节点指向同一节点 */
	SPIN_LOCK(q)
	assert(queue->next == NULL);
	if(q->tail) {
		q->tail->next = queue;
		q->tail = queue;
	} else {
		q->head = q->tail = queue;
	}
	SPIN_UNLOCK(q)
}

/* 将二级队列从全局队列中出队, 如果没有元素了就返回 NULL, 否则返回第一个元素,
 * 出列之后并未改变元素的 in_global 字段, 之所以这样做的原因参见 skynet_mq_pop 函数.
 * 此函数是线程安全的 */
struct message_queue * 
skynet_globalmq_pop() {
	struct global_queue *q = Q;

	/* 在自旋锁的情况下进行出列, 处理最后一个元素是将 head 和 tail 都重置为 NULL */
	SPIN_LOCK(q)
	struct message_queue *mq = q->head;
	if(mq) {
		q->head = mq->next;
		if(q->head == NULL) {
			assert(mq == q->tail);
			q->tail = NULL;
		}
		mq->next = NULL;
	}
	SPIN_UNLOCK(q)

	return mq;
}

/* 为一个服务创建消息队列, 队列创建时其 in_global 字段是 MQ_IN_GLOBAL 的,
 * 其原因在于队列创建的时期是服务刚创建出来而此时还没有初始化, 这样可以暂时不处理此时到来的消息 */
struct message_queue * 
skynet_mq_create(uint32_t handle) {
	struct message_queue *q = skynet_malloc(sizeof(*q));
	q->handle = handle;
	q->cap = DEFAULT_QUEUE_SIZE;
	q->head = 0;
	q->tail = 0;
	SPIN_INIT(q)
	// When the queue is create (always between service create and service init) ,
	// set in_global flag to avoid push it to global queue .
	// If the service init success, skynet_context_new will call skynet_mq_push to push it to global queue.
	q->in_global = MQ_IN_GLOBAL;
	q->release = 0;
	q->overload = 0;
	q->overload_threshold = MQ_OVERLOAD;
	q->queue = skynet_malloc(sizeof(struct skynet_message) * q->cap);
	q->next = NULL;

	return q;
}

/* 销毁队列, 回收队列的内存 */
static void 
_release(struct message_queue *q) {
	assert(q->next == NULL);
	SPIN_DESTROY(q)
	skynet_free(q->queue);
	skynet_free(q);
}

/* 获取此消息队列所属服务的 id */
uint32_t 
skynet_mq_handle(struct message_queue *q) {
	return q->handle;
}

/* 获取当前时刻消息队列的长度. 此函数是线程安全的. */
int
skynet_mq_length(struct message_queue *q) {
	int head, tail,cap;

	SPIN_LOCK(q)
	head = q->head;
	tail = q->tail;
	cap = q->cap;
	SPIN_UNLOCK(q)
	
	if (head <= tail) {
		return tail - head;
	}
	return tail + cap - head;
}

/* 检查并重置队列的负载. 返回非负值表示当前负载, 返回 0 表示未超过负载. */
int
skynet_mq_overload(struct message_queue *q) {
	if (q->overload) {
		int overload = q->overload;
		q->overload = 0;
		return overload;
	} 
	return 0;
}

/* 从消息队列中取出一条消息。返回 0 时表示获取成功，并且消息被复制到 message 中,
 * 返回 1 时表示队列已经为空了, message 保持不变. 此函数是线程安全的. 有两点值得说明一下:
 * 第一是当超过负载阈值时将在此设置负载值, 当队列为空时负载阈值重新设置为默认阈值.
 * 第二是仅当队列为空时, 才会设置 in_global 属性为 0, 原因是不为空的情况下, 消息分发器
 * 会在处理完之后自行将消息队列重新入队到全局队列中. */
int
skynet_mq_pop(struct message_queue *q, struct skynet_message *message) {
	int ret = 1;
	SPIN_LOCK(q)

	if (q->head != q->tail) {
		*message = q->queue[q->head++];
		ret = 0;
		int head = q->head;
		int tail = q->tail;
		int cap = q->cap;

		/* 当 head 超出数组界限时回绕到起点 */
		if (head >= cap) {
			q->head = head = 0;
		}
		int length = tail - head;
		if (length < 0) {
			length += cap;
		}
		/* 检查负载并且重新设置负载阈值 */
		while (length > q->overload_threshold) {
			q->overload = length;
			q->overload_threshold *= 2;
		}
	} else {
		// reset overload_threshold when queue is empty
		q->overload_threshold = MQ_OVERLOAD;
	}

	if (ret) {
		q->in_global = 0;
	}
	
	SPIN_UNLOCK(q)

	return ret;
}

/* 队列扩容, 扩展之后原先的元素被移动到数组的头部 */
static void
expand_queue(struct message_queue *q) {
	struct skynet_message *new_queue = skynet_malloc(sizeof(struct skynet_message) * q->cap * 2);
	int i;
	for (i=0;i<q->cap;i++) {
		new_queue[i] = q->queue[(q->head + i) % q->cap];
	}
	q->head = 0;
	q->tail = q->cap;
	q->cap *= 2;
	
	skynet_free(q->queue);
	q->queue = new_queue;
}

/* 向服务的消息队列中推入消息, 如果当前消息队列的 in_global 字段为 0 时, 将会被推入到全局队列中,
 * 此函数是线程安全的. */
void 
skynet_mq_push(struct message_queue *q, struct skynet_message *message) {
	assert(message);
	SPIN_LOCK(q)

	q->queue[q->tail] = *message;
	
	/* 自增 tail , 在超过容量大小时回绕, 如果又与 head 重叠将进行扩容 */
	if (++ q->tail >= q->cap) {
		q->tail = 0;
	}

	if (q->head == q->tail) {
		expand_queue(q);
	}

	if (q->in_global == 0) {
		q->in_global = MQ_IN_GLOBAL;
		skynet_globalmq_push(q);
	}
	
	SPIN_UNLOCK(q)
}

/* 初始化全局队列,
 * 此函数只能在系统启动时调用一次. 私下觉得 skynet_globalmq_init 更合适 */
void 
skynet_mq_init() {
	struct global_queue *q = skynet_malloc(sizeof(*q));
	/* 初始化 head 和 tail 为 NULL, 并且初始化锁为未加锁状态. */
	memset(q,0,sizeof(*q));
	SPIN_INIT(q);
	Q=q;
}

/* 标记消息队列为即将销毁, 每个队列只能调用此函数一次, 此函数是线程安全的.
 * 之所以存在此函数的原因是队列需要在服务对象销毁之后才能销毁, 队列会在下一次分发时实际删除. */
void 
skynet_mq_mark_release(struct message_queue *q) {
	SPIN_LOCK(q)
	assert(q->release == 0);
	q->release = 1;
	if (q->in_global != MQ_IN_GLOBAL) {
		skynet_globalmq_push(q);
	}
	SPIN_UNLOCK(q)
}

/* 真正执行销毁队列, 先将队列中的所有消息以 drop_func 函数方式清理, 再回收队列的内存 */
static void
_drop_queue(struct message_queue *q, message_drop drop_func, void *ud) {
	struct skynet_message msg;
	while(!skynet_mq_pop(q, &msg)) {
		drop_func(&msg, ud);
	}
	_release(q);
}

/* 实际销毁队列, 需要先标记为销毁才会实际销毁, 否则将重新入队, 暗含的假设就是调用此函数时队列必须
 * 已经从全局队列中取出. 此函数是线程安全的. */
void 
skynet_mq_release(struct message_queue *q, message_drop drop_func, void *ud) {
	SPIN_LOCK(q)
	
	if (q->release) {
		/* 先解锁的原因是 _drop_queue 调用的 skynet_mq_pop 会再次加锁 q
		   若不先解锁将会产生死锁. 当前的自旋锁不是可重入的 */
		SPIN_UNLOCK(q)
		_drop_queue(q, drop_func, ud);
	} else {
		skynet_globalmq_push(q);
		SPIN_UNLOCK(q)
	}
}
