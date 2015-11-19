#include "skynet.h"

#include "skynet_server.h"
#include "skynet_module.h"
#include "skynet_handle.h"
#include "skynet_mq.h"
#include "skynet_timer.h"
#include "skynet_harbor.h"
#include "skynet_env.h"
#include "skynet_monitor.h"
#include "skynet_imp.h"
#include "skynet_log.h"
#include "spinlock.h"
#include "atomic.h"

#include <pthread.h>

#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>

#ifdef CALLING_CHECK

/* 被 BEGIN 和 END 保护的代码段的执行是不并发的. 一旦发生并发会导致第二次加锁失败,
 * 从而使得 assert 失败, 程序退出. */
#define CHECKCALLING_BEGIN(ctx) if (!(spinlock_trylock(&ctx->calling))) { assert(0); }
#define CHECKCALLING_END(ctx) spinlock_unlock(&ctx->calling);
#define CHECKCALLING_INIT(ctx) spinlock_init(&ctx->calling);
#define CHECKCALLING_DESTROY(ctx) spinlock_destroy(&ctx->calling);
#define CHECKCALLING_DECL struct spinlock calling;

#else

#define CHECKCALLING_BEGIN(ctx)
#define CHECKCALLING_END(ctx)
#define CHECKCALLING_INIT(ctx)
#define CHECKCALLING_DESTROY(ctx)
#define CHECKCALLING_DECL

#endif

/* skynet 中的服务信息结构体 */
struct skynet_context {
	void * instance;                /* 服务实例, 由服务模块调用 *_create 方法获得 */
	struct skynet_module * mod;     /* 服务所属的模块 */
	void * cb_ud;                   /* 回调函数的用户自定义数据 */
	skynet_cb cb;                   /* 回调函数 */
	struct message_queue *queue;    /* 消息队列 */
	FILE * logfile;                 /* 日志文件, 当调试时可用于打印当前服务的所有消息处理过程 */
	char result[32];                /* 命令调用结果, 返回时供其它地方使用且保证内存有效性, skynet 内部保证
	                                 * 命令调用不会是并发的 */
	uint32_t handle;                /* 服务地址 */
	int session_id;                 /* 分配 session 的自增对象 */
	int ref;                        /* 引用计数器 */
	bool init;                      /* 初始化完毕的标记 */
	bool endless;                   /* 是否陷入无限循环的标记 */

	CHECKCALLING_DECL               /* 当需要对服务的消息处理和初始化校验是否线程封闭时, 定义的锁 */
};

/* 当前节点的全局状态信息的结构体 */
struct skynet_node {
	int total;                    /* 服务的数量 */
	int init;                     /* 当前节点全局信息是否初始化 */
	uint32_t monitor_exit;        /* 监控服务退出的服务地址 */
	pthread_key_t handle_key;     /* 保存当前工作线程正在为哪个服务处理消息的线程特定数据键 */
};

static struct skynet_node G_NODE;

/* 获取当前 skynet 节点中的服务数量, 目前被工作线程作为退出条件. */
int 
skynet_context_total() {
	return G_NODE.total;
}

/* 增加当前 skynet 节点中的服务数量 */
static void
context_inc() {
	ATOM_INC(&G_NODE.total);
}

/* 减少当前 skynet 节点中的服务数量 */
static void
context_dec() {
	ATOM_DEC(&G_NODE.total);
}

/* 获取当前线程的服务地址, 如果是工作线程则获取到的是正在处理的消息的目的地服务器地址,
 * 如果是定时器线程、socket 线程、监控线程或者主线程, 则为它们的线程类别的负值的 uint32 形式. */
uint32_t 
skynet_current_handle(void) {
	if (G_NODE.init) {
		void * handle = pthread_getspecific(G_NODE.handle_key);
		return (uint32_t)(uintptr_t)handle;
	} else {
		/* 在未初始化好节点全局信息之前, 线程特定数据还没有初始化, 也只有主线程在工作 */
		uint32_t v = (uint32_t)(-THREAD_MAIN);
		return v;
	}
}

/* 将 32 位无符号整数打印成带冒号前缀的 16 进制字符串.
 * 参数 str 为接收字符数组, 大小至少是 10 字节, id 为待转换的数据. */
static void
id_to_hex(char * str, uint32_t id) {
	int i;
	static char hex[16] = { '0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F' };
	str[0] = ':';
	for (i=0;i<8;i++) {
		str[i+1] = hex[(id >> ((7-i) * 4))&0xf];
	}
	str[9] = '\0';
}

/* 销毁消息时的额外信息结构体 */
struct drop_t {
	uint32_t handle;   /* 待销毁消息的目的地服务地址 */
};

/* 销毁 skynet 中的一个消息, 被销毁的消息不会被投递而是释放消息数据占用的内容, 并给源服务地址报告错误.
 * 参数 msg 是待销毁的消息, ud 是消息的额外信息, 其中包含消息的目的地服务地址. */
static void
drop_message(struct skynet_message *msg, void *ud) {
	struct drop_t *d = ud;
	skynet_free(msg->data);
	uint32_t source = d->handle;
	assert(source);
	// report error to the message source
	skynet_send(NULL, source, msg->source, PTYPE_ERROR, 0, NULL, 0);
}

/* 启动一个新的服务. 启动过程包括创建、初始化服务, 构建消息队列, 注册并获得服务地址.
 * 参数 name 为服务所属模块的 so 文件名字. param 为传递模块的启动参数.
 * 启动成功时返回服务对象指针, 失败时返回 NULL */
struct skynet_context * 
skynet_context_new(const char * name, const char *param) {
	struct skynet_module * mod = skynet_module_query(name);

	if (mod == NULL)
		return NULL;

	void *inst = skynet_module_instance_create(mod);
	if (inst == NULL)
		return NULL;
	struct skynet_context * ctx = skynet_malloc(sizeof(*ctx));
	CHECKCALLING_INIT(ctx)

	ctx->mod = mod;
	ctx->instance = inst;
	/* 初始化为 2 的原因在于防止初始化时遇到错误而可能退出服务导致释放服务对象内存,
	   我们需要服务对象内存至少保留到此函数结束. */
	ctx->ref = 2;
	ctx->cb = NULL;
	ctx->cb_ud = NULL;
	ctx->session_id = 0;
	ctx->logfile = NULL;

	ctx->init = false;
	ctx->endless = false;
	// Should set to 0 first to avoid skynet_handle_retireall get an uninitialized handle
	ctx->handle = 0;
	/* 一旦注册了服务就存在被别的线程调用 skynet_handle_retire 或者
	   skynet_handle_retireall 退出服务的危险 */
	ctx->handle = skynet_handle_register(ctx);
	struct message_queue * queue = ctx->queue = skynet_mq_create(ctx->handle);
	context_inc();

	/* 初始化过程一定是非并发的 */
	// init function maybe use ctx->handle, so it must init at last
	CHECKCALLING_BEGIN(ctx)
	int r = skynet_module_instance_init(mod, inst, ctx, param);
	CHECKCALLING_END(ctx)
	if (r == 0) {
		struct skynet_context * ret = skynet_context_release(ctx);
		if (ret) {
			ctx->init = true;
		}
		/* 无论如何都要将消息队列放入全局队列中去, 如果服务未能正确启动,
		 * 此消息队列会在分发消息时销毁. */
		skynet_globalmq_push(queue);
		if (ret) {
			skynet_error(ret, "LAUNCH %s %s", name, param ? param : "");
		}
		return ret;
	} else {
		skynet_error(ctx, "FAILED launch %s", name);
		uint32_t handle = ctx->handle;
		skynet_context_release(ctx);
		skynet_handle_retire(handle);
		struct drop_t d = { handle };
		skynet_mq_release(queue, drop_message, &d);
		return NULL;
	}
}

/* 为服务分配一个新的会话, 此函数会保证得到的会话 id 一定是正数且唯一的.
 * 参数 ctx 为需要分配会话 id 的服务. 函数返回新的唯一会话 id .
 * 此函数是非线程安全的, 因而只能非并发调用, skynet 通过让每个服务(服务的
 * 运行是单线程的)只能对自身进行分配会话来保证这一点. */
int
skynet_context_newsession(struct skynet_context *ctx) {
	// session always be a positive number
	int session = ++ctx->session_id;
	if (session <= 0) {
		ctx->session_id = 1;
		return 1;
	}
	return session;
}

/* 对服务对象的引用计数加一. 参数 ctx 是需要增加引用的服务.
 * 此函数是线程安全的. */
void 
skynet_context_grab(struct skynet_context *ctx) {
	ATOM_INC(&ctx->ref);
}

/* 保留一个服务, 被保留的服务保证当调用 skynet_handle_retire 并不真正退出服务.
 * 而是最后手动调用释放掉服务. 需要说明的是被保留的服务不会记录在 GNODE.total 中,
 * 因而不会阻止工作线程的退出. 参数 ctx 是需要被保留的服务. 此函数的用意在于一些地方
 * 需要在服务的生命周期之外持有服务对象的引用, 如 skynet_harbor.c 所做的. */
void
skynet_context_reserve(struct skynet_context *ctx) {
	skynet_context_grab(ctx);
	// don't count the context reserved, because skynet abort (the worker threads terminate) only when the total context is 0 .
	// the reserved context will be release at last.
	context_dec();
}

/* 销毁服务 ctx. 调用所属模块的卸载方法, 标记消息队列为需要释放以及释放服务对象内存,
 * 减少一个服务数量, 如果服务有自己的日志记录文件, 将其关闭. */
static void 
delete_context(struct skynet_context *ctx) {
	if (ctx->logfile) {
		fclose(ctx->logfile);
	}
	skynet_module_instance_release(ctx->mod, ctx->instance);
	skynet_mq_mark_release(ctx->queue);
	CHECKCALLING_DESTROY(ctx)
	skynet_free(ctx);
	context_dec();
}

/* 对服务对象的引用计数减一, 当引用计数为 0 时执行服务销毁. 参数 ctx 是需要减引用的服务.
 * 如果减引用后服务未销毁则返回服务对象指针, 如果销毁了则返回 NULL.
 * 此函数在绝大多数情况下是伴随 skynet_handle_grab 调用的, 一个加引用, 一个减引用.
 * 除了在卸载服务时会只调用此函数, 而使得引用计数为 0 执行销毁. */
struct skynet_context * 
skynet_context_release(struct skynet_context *ctx) {
	if (ATOM_DEC(&ctx->ref) == 0) {
		delete_context(ctx);
		return NULL;
	}
	return ctx;
}

/* 将一条消息推入到服务的消息队列中去. 如果服务不存在将返回 -1 , 否则将返回 0 . */
int
skynet_context_push(uint32_t handle, struct skynet_message *message) {
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL) {
		return -1;
	}
	skynet_mq_push(ctx->queue, message);
	skynet_context_release(ctx);

	return 0;
}

/* 标记服务为无限循环. 此函数是非线程安全的, 因而只能非并发调用. */
void 
skynet_context_endless(uint32_t handle) {
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL) {
		return;
	}
	ctx->endless = true;
	skynet_context_release(ctx);
}

/* 检查 handle 所表示的服务是否为远程服务, 如果是远程服务将返回 1 , 并且远程节点 id 将被填入
 * 入参 harbor 中, 否则返回 0 . 参数 ctx 没有用到. */
int 
skynet_isremote(struct skynet_context * ctx, uint32_t handle, int * harbor) {
	int ret = skynet_harbor_message_isremote(handle);
	if (harbor) {
		*harbor = (int)(handle >> HANDLE_REMOTE_SHIFT);
	}
	return ret;
}

/* 分发一条 skynet 中的消息. 此函数会设置当前工作线程的 handle_key 为当前正在处理的消息的服务句柄.
 * 如果打开了服务的调试日志, 还将记录 msg 消息. 调用服务的回调函数, 并且在必要时释放消息的 data 内存.
 * 调用此函数要求是非并发的.
 *
 *参数 ctx 是处理消息的服务, msg 是待处理的消息. */
static void
dispatch_message(struct skynet_context *ctx, struct skynet_message *msg) {
	assert(ctx->init);
	CHECKCALLING_BEGIN(ctx)
	pthread_setspecific(G_NODE.handle_key, (void *)(uintptr_t)(ctx->handle));
	int type = msg->sz >> MESSAGE_TYPE_SHIFT;
	size_t sz = msg->sz & MESSAGE_TYPE_MASK;
	if (ctx->logfile) {
		skynet_log_output(ctx->logfile, msg->source, type, msg->session, msg->data, sz);
	}
	/* 仅当回调函数返回 0 时才会释放 data 的内存 */
	if (!ctx->cb(ctx, ctx->cb_ud, type, msg->session, msg->source, msg->data, sz)) {
		skynet_free(msg->data);
	} 
	CHECKCALLING_END(ctx)
}

/* 一次性分发掉服务 ctx 的所有消息. 此函数仅在 skynet 出错需要尽快退出时使用. */
void 
skynet_context_dispatchall(struct skynet_context * ctx) {
	// for skynet_error
	struct skynet_message msg;
	struct message_queue *q = ctx->queue;
	while (!skynet_mq_pop(q,&msg)) {
		dispatch_message(ctx, &msg);
	}
}

/* 工作线程分发消息的函数, 此函数依据权重 weight 的值处理一条消息队列中的消息. 如果 weight >= 0
 * 处理的消息数量是消息队列的长度除以 2 的 weight 次方, 如果 weight < 0 , 将只处理一条.
 * 同时, 如果消息队列的服务已经退出了, 消息队列将销毁. 整个消息分发过程会利用 sm 监控是否陷入无限循环,
 * 并且还会检查消息队列是否过载. 如果传入的消息队列 q 非 NULL 值, 将处理此消息队列, 否则将先从全局队列中
 * 取得一条消息队列来处理, 若全局队列已经空了将不执行消息分发返回 NULL, 否则将执行消息分发并返回下一条
 * 带处理的消息队列. 值得注意的是如果处理完之后当前消息队列为空, 将不会推入到全局队里中.
 *
 * 参数: sm 为用于监控消息处理的监控对象, q 为将要被分发的消息队列, 可为 NULL, weight 为本次处理权重
 * 返回: 下一条将被分发的消息队列, 可能返回当前正在处理的队列, 也可能返回 NULL. */
struct message_queue * 
skynet_context_message_dispatch(struct skynet_monitor *sm, struct message_queue *q, int weight) {
	/* 若传入的消息队列为 NULL, 将从全局队列中取得, 并且只有在全局
	 * 队列中有消息队列的情况下才会执行分发. */
	if (q == NULL) {
		q = skynet_globalmq_pop();
		if (q==NULL)
			return NULL;
	}

	uint32_t handle = skynet_mq_handle(q);

	/* 若消息队列所属的服务已经退出, 将执行消息队列释放并且从全局消息队列中
	 * 取得下一条消息队列返回. */
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL) {
		struct drop_t d = { handle };
		skynet_mq_release(q, drop_message, &d);
		return skynet_globalmq_pop();
	}

	int i,n=1;
	struct skynet_message msg;

	/* 依据权重从消息队列中取得消息并处理, 如果消息队列被处理空了, 消息队列不会再次
	 * 推入全局队列并从全局消息队列中取得下一条消息队列并返回. */
	for (i=0;i<n;i++) {
		if (skynet_mq_pop(q,&msg)) {
			skynet_context_release(ctx);
			return skynet_globalmq_pop();
		} else if (i==0 && weight >= 0) {
			n = skynet_mq_length(q);
			n >>= weight;
		}
		int overload = skynet_mq_overload(q);
		if (overload) {
			skynet_error(ctx, "May overload, message queue length = %d", overload);
		}

		skynet_monitor_trigger(sm, msg.source , handle);

		/* 如果服务没有回调函数将直接释放消息中的内存, 说明 data 应该是堆内存. */
		if (ctx->cb == NULL) {
			skynet_free(msg.data);
		} else {
			dispatch_message(ctx, &msg);
		}

		skynet_monitor_trigger(sm, 0,0);
	}

	/* 如果全局队列不为空, 将当前消息队列推入全局消息队列并取得下一条消息队列返回,
	 * 否则返回当前消息队列. */
	assert(q == ctx->queue);
	struct message_queue *nq = skynet_globalmq_pop();
	if (nq) {
		// If global mq is not empty , push q back, and return next queue (nq)
		// Else (global mq is empty or block, don't push q back, and return q again (for next dispatch)
		skynet_globalmq_push(q);
		q = nq;
	} 
	skynet_context_release(ctx);

	return q;
}

/* 将 addr 字符串所表示的名字复制到 name 字符数组中去. 如果 addr 的长度大于
 * GLOBALNAME_LENGTH 将会被截断.
 *
 * 参数: name 为入参用于保存名字复制后的结果, addr 为待复制的名字 */
static void
copy_name(char name[GLOBALNAME_LENGTH], const char * addr) {
	int i;
	for (i=0;i<GLOBALNAME_LENGTH && addr[i];i++) {
		name[i] = addr[i];
	}
	for (;i<GLOBALNAME_LENGTH;i++) {
		name[i] = '\0';
	}
}

/* 查询名字 name 所表示的服务的地址. 如果 name 以冒号开始表示一个数字型服务名, 如果 name 以点号开头
 * 表示一个命名的服务名, 将从 handle 模块中查找服务的地址. 不支持其它形式的名字.
 *
 * 参数: context 是调用函数的服务, name 为待查询的名字
 * 返回: 名字所表示的服务的地址, 若不存在此名字将返回 0 . */
uint32_t 
skynet_queryname(struct skynet_context * context, const char * name) {
	switch(name[0]) {
	case ':':
		return strtoul(name+1,NULL,16);
	case '.':
		return skynet_handle_findname(name + 1);
	}
	skynet_error(context, "Don't support query global name %s",name);
	return 0;
}

/* 退出服务地址为 handle 的服务, 如果 handle 为 0 则退出 context 服务.
 * 此函数在存在服务退出监控的情况下, 会向其发送消息.
 *
 * 参数: context 为调用函数的服务, handle 为待退出的服务. */
static void
handle_exit(struct skynet_context * context, uint32_t handle) {
	if (handle == 0) {
		handle = context->handle;
		skynet_error(context, "KILL self");
	} else {
		skynet_error(context, "KILL :%0x", handle);
	}
	if (G_NODE.monitor_exit) {
		skynet_send(context, handle, G_NODE.monitor_exit, PTYPE_CLIENT, 0, NULL, 0);
	}
	skynet_handle_retire(handle);
}

// skynet command
/* 命令函数结构, 包含命令的名字以及命令函数指针 */
struct command_func {
	const char *name;
	/* 命令函数指针, context 为接收命令的服务, param 为命令的参数 */
	const char * (*func)(struct skynet_context * context, const char * param);
};

/* 发起定时任务命令. 将以 param 字符串所表示的厘秒数给服务 context 注册一个定时器事件.
 * 参数: context 是需要出发定时任务的服务, param 是定时器的触发时间, 单位为厘秒
 * 返回: 定时任务的会话 id */
static const char *
cmd_timeout(struct skynet_context * context, const char * param) {
	char * session_ptr = NULL;
	int ti = strtol(param, &session_ptr, 10);
	int session = skynet_context_newsession(context);
	skynet_timeout(context->handle, ti, session);
	sprintf(context->result, "%d", session);
	return context->result;
}

/* 为 context 服务注册并返回服务的名字, 如果 param 为 NULL 或者空字符串则返回冒号打头 16 进制的服务地址,
 * 如果 param 以点号打头, 则将点号之后的 param 字符串注册为服务名字并返回. 其它形式将返回 NULL.
 *
 * 参数: context 是需要注册名字的服务, param 为注册的名字, 形式如上.
 * 返回: 注册进去的名字或者冒号打头的 16 进制服务地址或者 NULL */
static const char *
cmd_reg(struct skynet_context * context, const char * param) {
	if (param == NULL || param[0] == '\0') {
		sprintf(context->result, ":%x", context->handle);
		return context->result;
	} else if (param[0] == '.') {
		return skynet_handle_namehandle(context->handle, param + 1);
	} else {
		skynet_error(context, "Can't register global name %s in C", param);
		return NULL;
	}
}

/* 依据服务名字查询服务地址, 仅当 param 以点号开头时才会查找服务的名字并返回冒号打头 16 进制的服务地址.
 * 其它形式将返回 NULL.
 *
 * 参数: context 是发起查询命令的服务, param 为待查询的名字
 * 返回: 查询到的冒号打头 16 进制的服务地址或者 NULL*/
static const char *
cmd_query(struct skynet_context * context, const char * param) {
	if (param[0] == '.') {
		uint32_t handle = skynet_handle_findname(param+1);
		if (handle) {
			sprintf(context->result, ":%x", handle);
			return context->result;
		}
	}
	return NULL;
}

/* 为 param 中包含的服务地址和名字注册名字. param 的形式是 .name :handle, 
 * 其中 name 为待注册的名字, handle 是服务的 16 进制地址. 若 param 不是此形式将返回 NULL.
 *
 * 参数: context 是发起注册命令的服务, param 为注册的名字和服务地址.
 * 返回: 成功时返回服务的名字, 失败时返回 NULL */
static const char *
cmd_name(struct skynet_context * context, const char * param) {
	int size = strlen(param);
	char name[size+1];
	char handle[size+1];
	sscanf(param,"%s %s",name,handle);
	if (handle[0] != ':') {
		return NULL;
	}
	uint32_t handle_id = strtoul(handle+1, NULL, 16);
	if (handle_id == 0) {
		return NULL;
	}
	if (name[0] == '.') {
		return skynet_handle_namehandle(handle_id, name + 1);
	} else {
		skynet_error(context, "Can't set global name %s in C", name);
	}
	return NULL;
}

/* 退出 context 服务.
 * 参数: context 是发起命令的服务也是待退出的服务, param 未使用.
 * 返回: NULL 表示无返回值 */
static const char *
cmd_exit(struct skynet_context * context, const char * param) {
	handle_exit(context, 0);
	return NULL;
}

/* 查询名字 param 所表示的服务的地址. 如果 param 以冒号开始表示一个数字型服务名,
 * 如果 param 以点号开头表示一个命名的服务名, 将从 handle 模块中查找服务的地址.
 * 不支持其它形式的名字. 此函数与 skynet_queryname 一模一样, 不过这个是供内部使用的.
 *
 * 参数: context 是调用函数的服务, param 为待查询的名字
 * 返回: 名字所表示的服务的地址, 若不存在此名字将返回 0 . */
static uint32_t
tohandle(struct skynet_context * context, const char * param) {
	uint32_t handle = 0;
	if (param[0] == ':') {
		handle = strtoul(param+1, NULL, 16);
	} else if (param[0] == '.') {
		handle = skynet_handle_findname(param+1);
	} else {
		skynet_error(context, "Can't convert %s to handle",param);
	}

	return handle;
}

/* 杀死 param 地址或名字所表示的服务. param 可以是以冒号打头的 16 进制服务地址或者
 * 以点号开头的命名服务.
 *
 * 参数: context 为发起命令的服务, param 是待杀死的服务地址, 形式如上.
 * 返回: NULL 表示无返回值. */
static const char *
cmd_kill(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle) {
		handle_exit(context, handle);
	}
	return NULL;
}

/* 启动并返回服务的冒号打头的 16 进制服务地址. 其中 param 是由空白符分开的模块名(mod) 和参数.
 * 其中的参数为启动服务所需要的参数, 可以没有.
 *
 * 参数: context 是发起命令的服务, param 是启动的服务模块和参数
 * 返回: 启动成功返回服务的地址, 不成功返回 NULL */
static const char *
cmd_launch(struct skynet_context * context, const char * param) {
	size_t sz = strlen(param);
	char tmp[sz+1];
	/* 复制的原因在 strsep 会改变字符串的内容 */
	strcpy(tmp,param);
	char * args = tmp;
	/* 模块与参数之间的分隔符可以包含多种空白符, 且参数中可以包含空格和制表符但不能包含回车换行符 */
	char * mod = strsep(&args, " \t\r\n");
	args = strsep(&args, "\r\n");
	struct skynet_context * inst = skynet_context_new(mod,args);
	if (inst == NULL) {
		return NULL;
	} else {
		id_to_hex(context->result, inst->handle);
		return context->result;
	}
}

/* 从 skynet 中获名为 param 的环境变量.
 * 参数: context 是发起命令的服务, 目前未使用, param 是环境变量的名字
 * 返回: 存在相应环境变量则返回其值, 不存在则返回 NULL */
static const char *
cmd_getenv(struct skynet_context * context, const char * param) {
	return skynet_getenv(param);
}

/* 向 skynet 中设置环境变量, param 是以空格隔开的键值对, 值可以是任意形式的字符串.
 * 参数: context 是发起命令的服务, param 是环境变量键值对
 * 返回: NULL 表示无返回值 */
static const char *
cmd_setenv(struct skynet_context * context, const char * param) {
	size_t sz = strlen(param);
	char key[sz+1];
	int i;
	/* 名字和服务之间必须用空格隔开, 不能只有键没有值, key 存储键, param 后半段存储值. */
	for (i=0;param[i] != ' ' && param[i];i++) {
		key[i] = param[i];
	}
	if (param[i] == '\0')
		return NULL;

	key[i] = '\0';
	param += i+1;
	
	skynet_setenv(key,param);
	return NULL;
}

/* 获取 skynet 的启动时间. 时间计算为从 1970 年 1 月 1 日 00:00 经过的秒数.
 * 此函数与 skynet_now 一同构成了墙上时钟.
 *
 * 参数: context 是发起命令的服务, param 未使用
 * 返回: 启动时间 */
static const char *
cmd_starttime(struct skynet_context * context, const char * param) {
	uint32_t sec = skynet_starttime();
	sprintf(context->result,"%u",sec);
	return context->result;
}

/* 查看 context 服务是否陷入消息处理无限循环中去了. 此函数还会重置服务的 endless 标识.
 * 参数: context 是需要查看的服务, param 未使用
 * 返回: 当确实陷入无限循环返回 "1" 字符串, 没有则返回 NULL  */
static const char *
cmd_endless(struct skynet_context * context, const char * param) {
	if (context->endless) {
		strcpy(context->result, "1");
		context->endless = false;
		return context->result;
	}
	return NULL;
}

/* 对 skynet 系统进行停机操作.
 * 参数: 均未使用
 * 返回: NULL 表示无返回值 */
static const char *
cmd_abort(struct skynet_context * context, const char * param) {
	skynet_handle_retireall();
	return NULL;
}

/* 获取或设置服务退出的监控服务. 如果 param 参数是 NULL 或者空字符串则为查询操作,
 * 不然则 param 需要是服务的冒号形式的 16 进制地址, 或者点号形式的服务名, 执行设置操作.
 *
 * 参数: context 为发起命令的服务, param 为空或者服务名、服务地址
 * 返回: 查询且存在返回服务地址, 不存在或者设置返回 NULL */
static const char *
cmd_monitor(struct skynet_context * context, const char * param) {
	uint32_t handle=0;
	if (param == NULL || param[0] == '\0') {
		if (G_NODE.monitor_exit) {
			// return current monitor serivce
			sprintf(context->result, ":%x", G_NODE.monitor_exit);
			return context->result;
		}
		return NULL;
	} else {
		handle = tohandle(context, param);
	}
	G_NODE.monitor_exit = handle;
	return NULL;
}

/* 获取 context 服务的消息队列大小.
 * 参数: context 待获取消息队列大小的服务, param 未使用
 * 返回: 消息队列大小 */
static const char *
cmd_mqlen(struct skynet_context * context, const char * param) {
	int len = skynet_mq_length(context->queue);
	sprintf(context->result, "%d", len);
	return context->result;
}

/* 给 param 所表示的服务开启调试日志, 日志内容是每一次的消息处理过程. param 可以是
 * 冒号打头的 16 进制服务地址或者点号打头的服务名. 当且仅当该服务之前没有开启调试日志时
 * 才会为其开启日志.
 *
 * 参数: context 为发起并执行命令的服务, param 是服务地址或服务名.
 * 返回: NULL 表示无返回值 */
static const char *
cmd_logon(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle == 0)
		return NULL;
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL)
		return NULL;
	FILE *f = NULL;
	/* 原先必须没有开启过调试日志才会打开, 并以原子方式比较赋值 */
	FILE * lastf = ctx->logfile;
	if (lastf == NULL) {
		f = skynet_log_open(context, handle);
		if (f) {
			if (!ATOM_CAS_POINTER(&ctx->logfile, NULL, f)) {
				// logfile opens in other thread, close this one.
				fclose(f);
			}
		}
	}
	skynet_context_release(ctx);
	return NULL;
}

/* 关闭 param 所表示的服务的调试日志. param 可以是冒号打头的 16 进制服务地址或者
 * 点号打头的服务名. 当且仅当该服务打开过调试日志才以线程安全的方式关闭.
 *
 * 参数: context 为发起命令的服务, param 为服务地址或服务名
 * 返回: NULL 表示无返回值 */
static const char *
cmd_logoff(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle == 0)
		return NULL;
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL)
		return NULL;
	/* 原先必须开启过调试日志才会关闭, 先以原子方式比较赋值 NULL */
	FILE * f = ctx->logfile;
	if (f) {
		// logfile may close in other thread
		if (ATOM_CAS_POINTER(&ctx->logfile, f, NULL)) {
			skynet_log_close(context, f, handle);
		}
	}
	skynet_context_release(ctx);
	return NULL;
}

/* 向一个服务发送信号. param 的前半段包含了服务地址, 后半段可选包含了信号.
 * 服务地址必须是冒号打头的 16 进制字符串. 信号可以是 10/8(需要以 0 开头)
 * /16(需要以 0x 开头) 进制的字符串, 中间用空格分割. 如果没有包含信号, 那么就使用默认的信号 0 .
 *
 * 参数: context 是发起命令的服务, param 是服务地址并以可选包含信号, 服务地址与信号之间用空格分割.
 * 返回: NULL 表示无返回值 */
static const char *
cmd_signal(struct skynet_context * context, const char * param) {
	uint32_t handle = tohandle(context, param);
	if (handle == 0)
		return NULL;
	struct skynet_context * ctx = skynet_handle_grab(handle);
	if (ctx == NULL)
		return NULL;
	/* 在 param 字符串中查询空格, 如果没有找到表示没有信号参数, 默认信号为 0 */
	param = strchr(param, ' ');
	int sig = 0;
	if (param) {
		sig = strtol(param, NULL, 0);
	}
	// NOTICE: the signal function should be thread safe.
	skynet_module_instance_signal(ctx->mod, ctx->instance, sig);

	skynet_context_release(ctx);
	return NULL;
}

/* 命令函数结构数组, 以所有字段为 NULL 的空结构表示结束 */
static struct command_func cmd_funcs[] = {
	{ "TIMEOUT", cmd_timeout },
	{ "REG", cmd_reg },
	{ "QUERY", cmd_query },
	{ "NAME", cmd_name },
	{ "EXIT", cmd_exit },
	{ "KILL", cmd_kill },
	{ "LAUNCH", cmd_launch },
	{ "GETENV", cmd_getenv },
	{ "SETENV", cmd_setenv },
	{ "STARTTIME", cmd_starttime },
	{ "ENDLESS", cmd_endless },
	{ "ABORT", cmd_abort },
	{ "MONITOR", cmd_monitor },
	{ "MQLEN", cmd_mqlen },
	{ "LOGON", cmd_logon },
	{ "LOGOFF", cmd_logoff },
	{ "SIGNAL", cmd_signal },
	{ NULL, NULL },
};

/* 向服务发送命令, 并执行命令得到结果.
 * 大部分命令利用了服务对象的 result 字符串作为返回值, 因而需要保证此函数的调用对于同一个服务对象是不并发的.
 * 要做到这一点并不难, skynet 只在服务(服务的运行是单线程的)刚启动时以及消息处理的回调函数中对自身调用此函数, 
 * 因而可以保证同一服务对象不并发. 还需要保证 result 的长度能够容纳结果字符串.
 *
 * 参数: context 为发送并执行命令的对象, cmd 为命令名称, param 为调用命令的参数.
 * 返回: 各个命令调用的结果, 当无法找到相应的命令时返回 NULL */
const char * 
skynet_command(struct skynet_context * context, const char * cmd , const char * param) {
	struct command_func * method = &cmd_funcs[0];
	while(method->name) {
		if (strcmp(cmd, method->name) == 0) {
			return method->func(context, param);
		}
		++method;
	}

	return NULL;
}

/* 在发送消息前对消息进行过滤, 包括可能的复制信息、分配会话 id 以及最后将消息类型嵌入到 sz 中去.
 * 其中 type 的高位有可能包含 PTYPE_TAG_DONTCOPY 和 PTYPE_TAG_ALLOCSESSION 标记来指示是否需要复制信息和
 * 分配会话 id, 最低 8 位则包含真实的消息类型.
 *
 * 参数: context 是发送消息的服务, type 为消息类型, session 为出参代表消息会话 id, 如果需要分配则传入时为 0,
 * 返回后将获得新的会话 id, *data 为出参, 代表消息内容, 如果需要复制消息, 返回后将执行复制后的消息头部;
 * *sz 为出参, 表示消息的大小, 返回后将会被嵌入消息类型于最高 8 位.
 *
 * 返回: 此函数无返回值, 返回值均由出参返回. */
static void
_filter_args(struct skynet_context * context, int type, int *session, void ** data, size_t * sz) {
	int needcopy = !(type & PTYPE_TAG_DONTCOPY);
	int allocsession = type & PTYPE_TAG_ALLOCSESSION;
	type &= 0xff;

	if (allocsession) {
		assert(*session == 0);
		*session = skynet_context_newsession(context);
	}

	if (needcopy && *data) {
		char * msg = skynet_malloc(*sz+1);
		memcpy(msg, *data, *sz);
		msg[*sz] = '\0';
		*data = msg;
	}

	*sz |= (size_t)type << MESSAGE_TYPE_SHIFT;
}

/* 将消息 *data 发送到目标服务 destination 的消息队列中去. 目标服务有可能是远程服务. 其中如果 source 参数为 0 的话,
 * 将使用 context 服务作为发送消息服务, 否则以 source 中服务地址为发送消息服务. 若 destination 服务为 0 , 将不发送消息
 * 而是直接返回 session 值, 也算作发送成功. type 的高位可能包含 PTYPE_TAG_ALLOCSESSION 和 PTYPE_TAG_DONTCOPY 两个标记,
 * 最低 8 位才是消息类型. 当需要分配 session 值时, 传入的 session 应该为 0 . 此函数对于消息内容 data 的内存管理的策略是
 * 仅当 type 带了 PTYPE_TAG_DONTCOPY 标记且发送失败时(函数会返回 -1)时才会释放 data 的内存, 其它情况不管成功失败都不会释放,
 * 所以此处的要求是如果 type 带了 PTYPE_TAG_DONTCOPY 标记则需要 data 是堆内存, 且失败后不需要调用者释放内存, 而成功时需要
 * 调用者释放, 如果 type 没有带这个标记则需要调用者自己管理 data 的内存.
 *
 * 参数: context 是函数调用者服务, 也可能是消息发送服务, source 消息来源服务地址, 当为 0 时发送服务为 context,
 * destination 为消息发送目的地服务, type 为消息类型, 可能包含 PTYPE_TAG_ALLOCSESSION 和 PTYPE_TAG_DONTCOPY 两个标记.
 * session 为消息的会话号, 为 0 时要求 type 包含 PTYPE_TAG_DONTCOPY 标记并进行会话号分配. *data 为消息内容, 注意点在上面描述.
 * sz 为消息内容大小, 不要超过 MESSAGE_TYPE_MASK .
 * 
 * 返回: 发送成功时返回会话号, 失败时返回 -1 . */
int
skynet_send(struct skynet_context * context, uint32_t source, uint32_t destination , int type, int session, void * data, size_t sz) {
	if ((sz & MESSAGE_TYPE_MASK) != sz) {
		skynet_error(context, "The message to %x is too large", destination);
		/* 如果 type 带有 PTYPE_TAG_DONTCOPY 则 data 必须是堆内存, 才可以安全释放 */
		if (type & PTYPE_TAG_DONTCOPY) {
			skynet_free(data);
		}
		return -1;
	}
	_filter_args(context, type, &session, (void **)&data, &sz);

	if (source == 0) {
		source = context->handle;
	}

	/* 目标服务地址为 0 , 直接返回会话号表示发送成功 */
	if (destination == 0) {
		return session;
	}
	
	/* 如果消息是远程消息将发送到 harbor 服务, 默认是成功的. 其得到的 data 一定是堆内存,
	 * 而且使用完之后之后必须释放, 对于本地消息发送也是一样的. */
	if (skynet_harbor_message_isremote(destination)) {
		struct remote_message * rmsg = skynet_malloc(sizeof(*rmsg));
		rmsg->destination.handle = destination;
		rmsg->message = data;
		rmsg->sz = sz;
		skynet_harbor_send(rmsg, source, session);
	} else {
		struct skynet_message smsg;
		smsg.source = source;
		smsg.session = session;
		smsg.data = data;
		smsg.sz = sz;
		
		if (skynet_context_push(destination, &smsg)) {
			/* 当发送失败时, type 没有 PTYPE_TAG_DONTCOPY 标记则释放的就是复制后的内存,
			 * 不然就是原始的 data 内存, 因而必须是堆内存. 从而得到结论如果需要复制消息,
			 * 原始 data 将有调用者进行内存管理, 而不用复制时内存由 skynet 底层统一管理. */
			skynet_free(data);
			return -1;
		}
	}
	return session;
}

/* 将消息 *data 发送到目标服务 *addr 的消息队列中去. *addr 可能是以冒号打头的 16 进制服务地址或者点号打头的服务名,
 * 也可能是一个全局服务名. 其中如果 source 参数没有的话, 将使用 context 服务作为发送消息服务, 否则以 source 中服务地址
 * 为发送消息服务. 若 destination 服务为 :0 , 将不发送消息而是直接返回 session 值, 也算作发送成功. 否则如果点号打头的服务
 * 不存在将返回 -1 表示失败. type 的高位可能包含 PTYPE_TAG_ALLOCSESSION 和 PTYPE_TAG_DONTCOPY 两个标记,
 * 最低 8 位才是消息类型. 当需要分配 session 值时, 传入的 session 应该为 0 . 此函数对于消息内容 data 的内存管理的策略是
 * 仅当 type 带了 PTYPE_TAG_DONTCOPY 标记且发送失败时(函数会返回 -1)时才会释放 data 的内存, 其它情况不管成功失败都不会释放,
 * 所以此处的要求是如果 type 带了 PTYPE_TAG_DONTCOPY 标记则需要 data 是堆内存, 且失败后不需要调用者释放内存, 而成功时需要
 * 调用者释放, 如果 type 没有带这个标记则需要调用者自己管理 data 的内存.
 *
 * 参数: context 是函数调用者服务, 也可能是消息发送服务, source 消息来源服务地址, 当为 0 时发送服务为 context,
 * addr 是目的地服务地址或者服务名, 可能是以冒号打头的 16 进制服务地址或者点号打头的服务名, 也可能是一个全局服务名,
 * type 为消息类型, 可能包含 PTYPE_TAG_ALLOCSESSION 和 PTYPE_TAG_DONTCOPY 两个标记.
 * session 为消息的会话号, 为 0 时要求 type 包含 PTYPE_TAG_DONTCOPY 标记并进行会话号分配. *data 为消息内容, 注意点在上面描述.
 * sz 为消息内容大小, 不要超过 MESSAGE_TYPE_MASK .
 * 
 * 返回: 发送成功时返回会话号, 失败时返回 -1 . */
int
skynet_sendname(struct skynet_context * context, uint32_t source, const char * addr , int type, int session, void * data, size_t sz) {
	if (source == 0) {
		source = context->handle;
	}
	uint32_t des = 0;
	if (addr[0] == ':') {
		des = strtoul(addr+1, NULL, 16);
	} else if (addr[0] == '.') {
		des = skynet_handle_findname(addr + 1);
		/* 如果服务不存在将失败 */
		if (des == 0) {
			/* 如果 type 带有 PTYPE_TAG_DONTCOPY 则 data 必须是堆内存, 才可以安全释放 */
			if (type & PTYPE_TAG_DONTCOPY) {
				skynet_free(data);
			}
			return -1;
		}
	} else {
		/* 以全局服务名发送远程消息, 默认是成功的. 其得到的 data 一定是堆内存,
		 * 而且使用完之后需要释放内存 */
		_filter_args(context, type, &session, (void **)&data, &sz);

		struct remote_message * rmsg = skynet_malloc(sizeof(*rmsg));
		copy_name(rmsg->destination.name, addr);
		rmsg->destination.handle = 0;
		rmsg->message = data;
		rmsg->sz = sz;

		skynet_harbor_send(rmsg, source, session);
		return session;
	}

	return skynet_send(context, source, des, type, session, data, sz);
}

/* 获取并返回服务的地址, 参数 ctx 为服务对象指针. */
uint32_t 
skynet_context_handle(struct skynet_context *ctx) {
	return ctx->handle;
}

/* 注册服务的回调函数. 参数 context 为服务对象地址, ud 为用户自定义数据, cb 为回调函数 */
void 
skynet_callback(struct skynet_context * context, void *ud, skynet_cb cb) {
	context->cb = cb;
	context->cb_ud = ud;
}

/* 向指定的服务发送消息. 参数 ctx 为接收消息的服务器对象指针, msg 为消息内容, 必须是堆内存,
 * sz 为消息内容大小， source 为发送服务的地址, type 为消息的类型, session 是消息的会话 */
void
skynet_context_send(struct skynet_context * ctx, void * msg, size_t sz, uint32_t source, int type, int session) {
	struct skynet_message smsg;
	smsg.source = source;
	smsg.session = session;
	smsg.data = msg;
	/* 将消息的类型编码到消息的大小的高 8 位 */
	smsg.sz = sz | (size_t)type << MESSAGE_TYPE_SHIFT;

	skynet_mq_push(ctx->queue, &smsg);
}

/* 执行全局信息初始化, 标识此时系统中还没有启动服务, 没有配置监控服务退出的服务,
 * 并生成一个线程特定数据用于工作线程获取自己正在处理的消息所属的服务. */
void 
skynet_globalinit(void) {
	G_NODE.total = 0;
	G_NODE.monitor_exit = 0;
	G_NODE.init = 1;
	if (pthread_key_create(&G_NODE.handle_key, NULL)) {
		fprintf(stderr, "pthread_key_create failed");
		exit(1);
	}
	// set mainthread's key
	skynet_initthread(THREAD_MAIN);
}

/* 执行全局信息反初始化, 其实就是删除线程特定数据 */
void 
skynet_globalexit(void) {
	pthread_key_delete(G_NODE.handle_key);
}

/* 完成对当前线程的初始化. 传入参数为当前线程所属类别. 此函数应该在线程初始化的最后一步调用. */
void
skynet_initthread(int m) {
	/* 将当前线程所属类别转化为一个区别于任何服务 id 的值, 并保存至
	   线程特定数据 handle_key 中以备后续取出使用. */
	uintptr_t v = (uint32_t)(-m);
	pthread_setspecific(G_NODE.handle_key, (void *)v);
}

