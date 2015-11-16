#include "skynet_log.h"
#include "skynet_timer.h"
#include "skynet.h"
#include "skynet_socket.h"
#include <string.h>
#include <time.h>

/* 在 logpath 下以二进制追加的方式打开一个名为 [handle].log 的日志文件, 并打印当前时间.
 * 如果没有配置 logpath 将直接返回 NULL.
 * 打开成功时返回文件流对象指针, 失败时返回 NULL. 参数 ctx 为调用此函数的服务,
 * handle 为接收命令的服务的地址. */
FILE * 
skynet_log_open(struct skynet_context * ctx, uint32_t handle) {
	const char * logpath = skynet_getenv("logpath");
	if (logpath == NULL)
		return NULL;
	size_t sz = strlen(logpath);
	char tmp[sz + 16];
	sprintf(tmp, "%s/%08x.log", logpath, handle);
	FILE *f = fopen(tmp, "ab");
	if (f) {
		uint32_t starttime = skynet_gettime_fixsec();
		uint32_t currenttime = skynet_gettime();
		time_t ti = starttime + currenttime/100;
		skynet_error(ctx, "Open log file %s", tmp);
		fprintf(f, "open time: %u %s", currenttime, ctime(&ti));
		fflush(f);
	} else {
		skynet_error(ctx, "Open log file %s fail", tmp);
	}
	return f;
}

/* 关闭日志文件流对象, 关闭前会打印当前时间. 参数 ctx 为调用此函数的服务, f 为日志文件流对象,
 * handle 为接收命令的服务地址. */
void
skynet_log_close(struct skynet_context * ctx, FILE *f, uint32_t handle) {
	skynet_error(ctx, "Close log file :%08x", handle);
	fprintf(f, "close time: %u\n", skynet_gettime());
	fclose(f);
}

/* 以 16 进制方式输出内存块中的值, 其中每个字节是两个 16 进制数字.
 * 参数 f 为输出文件, buffer 为代输出内存起始地址, sz 为输出的字节数. */
static void
log_blob(FILE *f, void * buffer, size_t sz) {
	size_t i;
	uint8_t * buf = buffer;
	for (i=0;i!=sz;i++) {
		fprintf(f, "%02x", buf[i]);
	}
}

/* 输出 socket 消息至日志文件中. 同时会打印消息的元信息.
 * 参数 f 为日志文件, message 代表一个 socket 消息, sz 是消息内容的大小. */
static void
log_socket(FILE * f, struct skynet_socket_message * message, size_t sz) {
	fprintf(f, "[socket] %d %d %d ", message->type, message->id, message->ud);

	/* socket 消息的格式有两种: 1 以字符串形式拼接在 message 结构的尾部.
	   2 存储在 buffer 字段中, 而 sz 则为 ud. 具体参考 socket_server.c */
	if (message->buffer == NULL) {
		const char *buffer = (const char *)(message + 1);
		sz -= sizeof(*message);
		/* 确保 sz 确实为字符串的长度, 否则有可能多打印些空格 */
		const char * eol = memchr(buffer, '\0', sz);
		if (eol) {
			sz = eol - buffer;
		}
		fprintf(f, "[%*s]", (int)sz, (const char *)buffer);
	} else {
		sz = message->ud;
		log_blob(f, message->buffer, sz);
	}
	fprintf(f, "\n");
	fflush(f);
}

/* 将 skynet 消息输出到日志中. 函数会记录消息本身的元信息以及消息的内容, 记录会被分成网络消息和其它消息分别记录.
 * 参数 f 为输出日志文件, source 为消息来源, type 为消息类型, session 为消息的会话编号,
 * buffer 为消息内容, sz 为消息大小. */
void 
skynet_log_output(FILE *f, uint32_t source, int type, int session, void * buffer, size_t sz) {
	if (type == PTYPE_SOCKET) {
		log_socket(f, buffer, sz);
	} else {
		uint32_t ti = skynet_gettime();
		fprintf(f, ":%08x %d %d %u ", source, type, session, ti);
		log_blob(f, buffer, sz);
		fprintf(f,"\n");
		fflush(f);
	}
}
