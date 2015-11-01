#include "skynet.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

/* 日志器结构体 */
struct logger {
	FILE * handle;     /* 输出文件对象指针 */
	int close;         /* 输出文件的状态, 1 表示需要关闭, 0 表示不再需要关闭 */
};

/* 实例化日志器, 每次调用都创建一个新的日志器, 此时只分配了内存, 并执行了最初步的初始化,
 * 也没有关联输出文件. 函数签名为标准的服务实例化签名. 成功时返回对象指针, 失败时返回 NULL .
 * 此函数只在实例化服务时调用一次. */
struct logger *
logger_create(void) {
	/* [fix]应该先检查 inst 内存是否分配成功, 才能接着初始化. [/fix] */
	struct logger * inst = skynet_malloc(sizeof(*inst));
	inst->handle = NULL;
	inst->close = 0;
	return inst;
}

/* 销毁日志器对象, 函数签名为标准的服务反初始化签名.
 * 此函数会在日志服务正常或异常退出时调用一次. */
void
logger_release(struct logger * inst) {
	/* 只有正确初始化之后, 且的确打开某个输出文件时
	   (此时 close 标记为 1 )才会关闭输出文件. */
	if (inst->close) {
		fclose(inst->handle);
	}
	skynet_free(inst);
}

/* 日志服务的回调函数, 以 8 位十六进制方式输出源服务 handle, 紧接着输出日志消息
 * 到当前日志器的输出文件中. 签名为标准服务回调函数签名. 当写入成功则返回 0, 失败返回非 0 值.
 * 此函数不是线程安全的. */
static int
_logger(struct skynet_context * context, void *ud, int type, int session, uint32_t source, const void * msg, size_t sz) {
	struct logger * inst = ud;
	fprintf(inst->handle, "[:%08x] ",source);
	fwrite(msg, sz , 1, inst->handle);
	fprintf(inst->handle, "\n");
	fflush(inst->handle);

	return 0;
}

/* 对启动的日志服务进行最终初始化, 初始化的工作包括打开输出文件, 如果没有提供则默认为标准输出.
 * 设置回调函数以及为此服务注册唯一名字 .logger . 签名为标准的服务初始化签名.
 * 传入的 parm 参数是日志输出的目标文件. 初始化成功时返回 0 , 失败时返回 1 .
 * 此函数只在服务初始化时调用一次. */
int
logger_init(struct logger * inst, struct skynet_context *ctx, const char * parm) {
	if (parm) {
		inst->handle = fopen(parm,"w");
		if (inst->handle == NULL) {
			return 1;
		}
		inst->close = 1;
	} else {
		inst->handle = stdout;
	}
	if (inst->handle) {
		skynet_callback(ctx, inst, _logger);
		skynet_command(ctx, "REG", ".logger");
		return 0;
	}
	return 1;
}
