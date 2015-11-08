#include "skynet.h"
#include "skynet_server.h"
#include "skynet_imp.h"
#include "skynet_mq.h"
#include "skynet_handle.h"
#include "skynet_module.h"
#include "skynet_timer.h"
#include "skynet_monitor.h"
#include "skynet_socket.h"
#include "skynet_daemon.h"
#include "skynet_harbor.h"

#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* 所有工作线程的总监视对象 */
struct monitor {
	int count;                        /* 工作线程的数量 */
	struct skynet_monitor ** m;       /* 所有工作线程的死循环监控对象指针数组 */
	pthread_cond_t cond;              /* 唤醒工作线程的条件变量 */
	pthread_mutex_t mutex;            /* 与条件变量相关联的互斥锁 */
	int sleep;                        /* 睡眠的工作线程数量 */
	int quit;                         /* 是否退出工作线程的标记, 0 为不退出, 1 为退出 */
};

/* 调用工作线程函数的参数结构体 */
struct worker_parm {
	struct monitor *m;                /* 总监视对象指针 */
	int id;                           /* 当前工作线程的编号 */
	int weight;                       /* 权重值, 若为正数越大表示一次性处理的消息越少,
	                                     若为负数一次仅处理一条数据 */
};

#define CHECK_ABORT if (skynet_context_total()==0) break;

/* 以默认属性创建线程, 新线程将执行 start_routine 函数, arg 为传递给该函数的参数.
 * 如果创建失败将退出程序.*/
static void
create_thread(pthread_t *thread, void *(*start_routine) (void *), void *arg) {
	if (pthread_create(thread,NULL, start_routine, arg)) {
		fprintf(stderr, "Create thread failed");
		exit(1);
	}
}

/* 以 busy 为活跃线程的下限(至少为 busy+1), 唤醒任意一条工作线程. */
static void
wakeup(struct monitor *m, int busy) {
	if (m->sleep >= m->count - busy) {
		// signal sleep worker, "spurious wakeup" is harmless
		pthread_cond_signal(&m->cond);
	}
}

/* socket 线程函数, 在初始化之后以阻塞方式等待 socket 事件(包括对 socket 模块发送命令), 直到有明确退出信号或者
 * 所有服务都已经退出. 此函数会保证至少有一条工作线程来处理到达的 socket 事件.
 * 必须说明的是, 虽然 socket 线程以阻塞方式等待 socket 事件, 但 socket 连接上的读写都是非阻塞的. */
static void *
thread_socket(void *p) {
	struct monitor * m = p;
	skynet_initthread(THREAD_SOCKET);
	for (;;) {
		/* 阻塞等待 socket 事件, 如果没有 socket 事件线程将一直阻塞, 
		   当返回值为 0 将退出此线程. 当返回值小于 0 表示信息不完整将检查服务状态,
		   当服务都退出时退出此线程, 否则继续轮询. 当返回大于 0 时, 确保至少
		   有一条工作线程来处理此消息. */
		int r = skynet_socket_poll();
		if (r==0)
			break;
		if (r<0) {
			CHECK_ABORT
			continue;
		}
		wakeup(m,0);
	}
	return NULL;
}

/* 释放 struct monitor 对象, 执行一系列反初始化操作, 此函数将回收所有的 struct skynet_monitor 对象内存
 * 及其指针数组的内存, 以及销毁条件变量和互斥锁, 并回收 struct monitor 对象本身的内存.
 * 此函数只会在退出程序时运行一次. */
static void
free_monitor(struct monitor *m) {
	int i;
	int n = m->count;
	for (i=0;i<n;i++) {
		skynet_monitor_delete(m->m[i]);
	}
	pthread_mutex_destroy(&m->mutex);
	pthread_cond_destroy(&m->cond);
	skynet_free(m->m);
	skynet_free(m);
}

/* 监控线程函数, 在初始化之后以 5 秒的间隔检查, 每次检查所有工作线程是否处于无限循环中. */
static void *
thread_monitor(void *p) {
	struct monitor * m = p;
	int i;
	int n = m->count;
	skynet_initthread(THREAD_MONITOR);
	for (;;) {
		/* 此处检查所有服务是否退出的原因是当内层循环由于 break 退出时会运行到此处. */
		CHECK_ABORT
		for (i=0;i<n;i++) {
			skynet_monitor_check(m->m[i]);
		}
		for (i=0;i<5;i++) {
			CHECK_ABORT
			sleep(1);
		}
	}

	return NULL;
}

/* 定时线程的函数, 在初始化之后以 2.5 毫秒的时间间隔更新定时器、触发定时事件、唤醒任意睡眠的工作线程
 * 并且检查退出条件. 当退出时将通知 socket 线程和工作线程退出. */
static void *
thread_timer(void *p) {
	struct monitor * m = p;
	skynet_initthread(THREAD_TIMER);
	for (;;) {
		/* 没运行四次将会更新一次时间, 因为时间单位是厘秒, 而运行间隔是 2.5 毫秒 */
		skynet_updatetime();
		CHECK_ABORT
		wakeup(m,m->count-1);
		usleep(2500);
	}
	/* socket 线程有可能在阻塞等待 socket 事件而无法检查退出条件, 故需要唤醒 */
	// wakeup socket thread
	skynet_socket_exit();
	/* 工作线程有可能因为等待条件变量而阻塞, 故需要全部唤醒. */
	// wakeup all worker thread
	pthread_mutex_lock(&m->mutex);
	/* 之所以将设置工作线程退出标记以及唤醒所有工作线程的工作放在同步块中执行,
	   是为了防止与工作线程的条件等待产生竞争. 若不如此, 有可能唤醒消息发出之后那瞬间工作线程刚好处于
	   条件等待之前, 这样工作线程就会错过唤醒消息而用于处于条件等待状态. */
	m->quit = 1;
	pthread_cond_broadcast(&m->cond);
	pthread_mutex_unlock(&m->mutex);
	return NULL;
}

/* 工作线程函数, 在初始化之后此函数在一个循环中每次依照自身的权重处理一条消息队列中的若干消息,
 * 并返回下一条消息队列供下一次循环处理. 当没有消息队列需要处理时就增加睡眠工作线程数量
 * 并以阻塞方式等待再次有需要处理的消息队列(实现为等待条件变量). 循环的退出条件是总监视对象的退出标记. */
static void *
thread_worker(void *p) {
	struct worker_parm *wp = p;
	int id = wp->id;
	int weight = wp->weight;
	struct monitor *m = wp->m;
	struct skynet_monitor *sm = m->m[id];
	skynet_initthread(THREAD_WORKER);
	struct message_queue * q = NULL;
	while (!m->quit) {
		/* 处理 q 中的若干消息, 并返回下一条消息队列, 若没有了消息队列返回 NULL,
		   调用分发函数同时会触发相应的无限循环监视. */
		q = skynet_context_message_dispatch(sm, q, weight);
		if (q == NULL) {
			if (pthread_mutex_lock(&m->mutex) == 0) {
				++ m->sleep;
				/* 此处必须检查退出标记, 若不检查, 一旦进入阻塞等待状态将永远无法被唤醒.
				   因为定时线程在设置完退出标记就退出了. */
				// "spurious wakeup" is harmless,
				// because skynet_context_message_dispatch() can be call at any time.
				if (!m->quit)
					pthread_cond_wait(&m->cond, &m->mutex);
				-- m->sleep;
				if (pthread_mutex_unlock(&m->mutex)) {
					fprintf(stderr, "unlock mutex error");
					exit(1);
				}
			}
		}
	}
	return NULL;
}

/* 当 skynet 的初始化完毕时, 调用此函数启动监视线程、定时线程、socket 线程和工作线程开始处理消息.
 * 只有当所有线程都停止工作之后, start 函数才会返回. 传入的参数为工作线程的数量. */
static void
start(int thread) {
	pthread_t pid[thread+3];

	/* 初始化总监控对象 */
	struct monitor *m = skynet_malloc(sizeof(*m));
	memset(m, 0, sizeof(*m));
	m->count = thread;
	m->sleep = 0;

	/* 为每条工作线程分配一个监控对象, 并初始化互斥锁和条件变量 */
	m->m = skynet_malloc(thread * sizeof(struct skynet_monitor *));
	int i;
	for (i=0;i<thread;i++) {
		m->m[i] = skynet_monitor_new();
	}
	if (pthread_mutex_init(&m->mutex, NULL)) {
		fprintf(stderr, "Init mutex error");
		exit(1);
	}
	if (pthread_cond_init(&m->cond, NULL)) {
		fprintf(stderr, "Init cond error");
		exit(1);
	}

	/* 创建所有线程, 创建的先后顺序影响不大. */
	create_thread(&pid[0], thread_monitor, m);
	create_thread(&pid[1], thread_timer, m);
	create_thread(&pid[2], thread_socket, m);

    /* 权重值为负数每次处理消息队列中一条消息就转到下一条消息队列,
       为 0 会将当前消息队列中所有消息处理掉, 1 则相应减半, 2 则为 1/4, 3 为 1/8.
       将负值放在前面的好处在于所有消息队列均有机会被均衡执行, 而当线程更多时可以
       让它们一次充分处理一条消息队列, 随着有更多线程再稳步递减一次处理的数量.
       此方法能兼顾吞吐量和响应性. */
	static int weight[] = { 
		-1, -1, -1, -1, 0, 0, 0, 0,
		1, 1, 1, 1, 1, 1, 1, 1, 
		2, 2, 2, 2, 2, 2, 2, 2, 
		3, 3, 3, 3, 3, 3, 3, 3, };
	struct worker_parm wp[thread];
	for (i=0;i<thread;i++) {
		wp[i].m = m;
		wp[i].id = i;
		/* 确保当 thread 多于 weight 数组长度时不会溢出 */
		if (i < sizeof(weight)/sizeof(weight[0])) {
			wp[i].weight= weight[i];
		} else {
			wp[i].weight = 0;
		}
		create_thread(&pid[i+3], thread_worker, &wp[i]);
	}

	/* 等待上面所创建的线程退出, 也意味着整个系统退出. */
	for (i=0;i<thread+3;i++) {
		pthread_join(pid[i], NULL); 
	}

	free_monitor(m);
}

/* 启动 skynet 的第二个服务,(第一个是日志服务) 此服务负责将所有其它服务启动起来.
 * 此服务默认运行 bootstrap.lua脚本. 在失败时尽快将缓存的日志信息输出, 并退出进程. */
static void
bootstrap(struct skynet_context * logger, const char * cmdline) {
	int sz = strlen(cmdline);
	/* 保证有足够的长度即便是 cmdline 中间没有空格, 也能够容纳整个字符串. */
	char name[sz+1];
	char args[sz+1];
	sscanf(cmdline, "%s %s", name, args);
	struct skynet_context *ctx = skynet_context_new(name, args);
	if (ctx == NULL) {
		skynet_error(NULL, "Bootstrap error : %s\n", cmdline);
		skynet_context_dispatchall(logger);
		exit(1);
	}
}

/* 依据配置对当前 skynet 节点启动. 首先依据配置将进程作为守护进程, 再对组件单例对象进行初始化,
 * 然后启动日志服务, 随后默认执行脚本 bootstrap.lua 启动其它服务.
 * 最后启动各种线程来完成工作, 其中任何一项工作发生错误都会导致进程退出.
 * 当这些线程都退出时, 再退出 harbor 服务. 最后结束此守护进程(如果是的话) */
void 
skynet_start(struct skynet_config * config) {
	if (config->daemon) {
		if (daemon_init(config->daemon)) {
			exit(1);
		}
	}

	/* 初始化各个组件单例对象 */
	skynet_harbor_init(config->harbor);
	skynet_handle_init(config->harbor);
	skynet_mq_init();
	skynet_module_init(config->module_path);
	skynet_timer_init();
	skynet_socket_init();

	struct skynet_context *ctx = skynet_context_new(config->logservice, config->logger);
	if (ctx == NULL) {
		fprintf(stderr, "Can't launch %s service\n", config->logservice);
		exit(1);
	}

	/* bootstrap 脚本完成其它所有的服务启动工作 */
	bootstrap(ctx, config->bootstrap);

	/* 启动线程来处理 socket 事件、定时任务和服务间发送的消息 */
	start(config->thread);

	/* [ck]虽然此时 socket 线程已经退出, 也不会再等待 socket 事件了,[/ck]
	   但是 struct socket_server 对象还没有被销毁, 依然可以给它发消息, 一旦此对象销毁了
	   而再向它发消息, 必然会导致段错误. */
	// harbor_exit may call socket send, so it should exit before socket_free
	skynet_harbor_exit();
	skynet_socket_free();
	if (config->daemon) {
		daemon_exit(config->daemon);
	}
}
