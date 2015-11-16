#include "skynet.h"
#include "skynet_handle.h"
#include "skynet_mq.h"
#include "skynet_server.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define LOG_MESSAGE_SIZE 256

/* 向名字为 logger 的日志服务发送日志消息, 此函数中 context 是发送服务对象, msg 为格式化字符串,
 * 后边的不定参数是其替换对象. 函数要求调用自行管理格式字符串和替换对象的内存.
 * 函数保证能够完整格式化消息, 但是不保证消息一定能够输出, 因为有可能日志服务已经被销毁了. */
void 
skynet_error(struct skynet_context * context, const char *msg, ...) {
	static uint32_t logger = 0;
	if (logger == 0) {
		logger = skynet_handle_findname("logger");
	}
	if (logger == 0) {
		return;
	}

	char tmp[LOG_MESSAGE_SIZE];
	char *data = NULL;           /* data 为发送的数据, 必须指向堆内存 */

	va_list ap;

	/* 将消息格式化写入 tmp 数组中, 仅当 len 小于 数组长度时表示全部写入 */
	va_start(ap,msg);
	/* [fix]有可能有编码错误, 返回值 len 将为负数. [/fix] */
	int len = vsnprintf(tmp, LOG_MESSAGE_SIZE, msg, ap);
	va_end(ap);
	if (len < LOG_MESSAGE_SIZE) {
		data = skynet_strdup(tmp);
	} else {
		/* 循环直到能够完全写入, 注意堆内存管理 */
		int max_size = LOG_MESSAGE_SIZE;
		for (;;) {
			max_size *= 2;
			data = skynet_malloc(max_size);
			va_start(ap,msg);
			len = vsnprintf(data, max_size, msg, ap);
			va_end(ap);
			if (len < max_size) {
				break;
			}
			skynet_free(data);
		}
	}


	struct skynet_message smsg;
	if (context == NULL) {
		smsg.source = 0;
	} else {
		smsg.source = skynet_context_handle(context);
	}
	smsg.session = 0;
	smsg.data = data;
	smsg.sz = len | ((size_t)PTYPE_TEXT << MESSAGE_TYPE_SHIFT);
	/* [fix]应该检查错误, 并回收 data 的内存. [/fix] */
	skynet_context_push(logger, &smsg);
}

