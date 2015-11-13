#ifndef SKYNET_IMP_H
#define SKYNET_IMP_H

/* skynet 启动配置参数 */
struct skynet_config {
	int thread;                     /* 工作线程的数量 (默认为 8) */
	int harbor;                     /* harbor id (默认为 1) */
	const char * daemon;            /* 守护进程 pid 文件名 */
	const char * module_path;       /* C 服务的路径 (默认为 ./cservice/?.so) */
	const char * bootstrap;         /* 启动整个 skynet 系统的入口服务 (默认为 snlua bootstrap) */
	const char * logger;            /* 日志文件的路径, nil 表示标准输出 */
	const char * logservice;        /* 日志服务 (默认为 logger) */
};

/* 线程的类别, 作为线程初始化的参数, 它们的负值将被转为 unit32 整数并与服务句柄一样设置在线程特定数据中,
 * 因而需要它们的负值有别于服务的 handle */
#define THREAD_WORKER 0      /* 0 */
#define THREAD_MAIN 1        /* -1 = 0xFFFFFFFF */
#define THREAD_SOCKET 2      /* -2 = 0xFFFFFFFE */
#define THREAD_TIMER 3       /* -3 = 0xFFFFFFFD */
#define THREAD_MONITOR 4     /* -4 = 0xFFFFFFFC */

void skynet_start(struct skynet_config * config);

#endif
