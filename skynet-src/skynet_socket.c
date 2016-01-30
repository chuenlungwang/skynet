#include "skynet.h"

#include "skynet_socket.h"
#include "socket_server.h"
#include "skynet_server.h"
#include "skynet_mq.h"
#include "skynet_harbor.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

static struct socket_server * SOCKET_SERVER = NULL;

/* 初始化套接字模块, 创建单例套接字服务器 */
void 
skynet_socket_init() {
	SOCKET_SERVER = socket_server_create();
}

/* 向套接字服务器发送退出命令, 这将导致主循环函数 skynet_socket_poll 返回 0 , 从而令 socket 线程退出,
 * 整个过程是一个异步的过程. 需要注意的是, 退出函数并没有销毁套接字服务器的内存. */
void
skynet_socket_exit() {
	socket_server_exit(SOCKET_SERVER);
}

/* 销毁套接字服务器以及所有组件的内存. 这是一个同步的过程, 应当在 socket 线程退出之后才执行. */
void
skynet_socket_free() {
	socket_server_release(SOCKET_SERVER);
	SOCKET_SERVER = NULL;
}

// mainloop thread
/* 将套接字信息发送到对应的服务区, 消息内容及服务句柄都在 result 参数中. 消息内容分为填充的以及非填充的,
 * 可填充的信息内容是不需要释放的内存, 内容为字符串, 且其大小不应该超过 128 个字节; 不可填充的信息内容是需要释放的内存,
 * 这个工作交由后面的流程完成;
 *
 * 参数: type 是 skynet_socket_message 的类型, 定义在 skynet_socket.h 中; padding 表示是否需要填充; result 中包含了数据内容和大小; */
static void
forward_message(int type, bool padding, struct socket_message * result) {
	struct skynet_socket_message *sm;
	size_t sz = sizeof(*sm);
	if (padding) {
		if (result->data) {
			size_t msg_sz = strlen(result->data);
			if (msg_sz > 128) {
				msg_sz = 128;
			}
			sz += msg_sz;
		} else {
			result->data = "";
		}
	}
	sm = (struct skynet_socket_message *)skynet_malloc(sz);
	sm->type = type;
	sm->id = result->id;
	sm->ud = result->ud;
	if (padding) {
		sm->buffer = NULL;
		memcpy(sm+1, result->data, sz - sizeof(*sm));
	} else {
		sm->buffer = result->data;
	}

	struct skynet_message message;
	message.source = 0;
	message.session = 0;
	message.data = sm;
	message.sz = sz | ((size_t)PTYPE_SOCKET << MESSAGE_TYPE_SHIFT);
	
	if (skynet_context_push((uint32_t)result->opaque, &message)) {
		// todo: report somewhere to close socket
		// don't call skynet_socket_close here (It will block mainloop)
		skynet_free(sm->buffer);
		skynet_free(sm);
	}
}

/* 套接字模块的主函数, 不断处理各种套接字命令和套接字 I/O 事件, 将处理的结果发送给对应的服务.
 * 函数返回 0 表示需要退出套接字模块线程, 返回 -1 表示套接字模块处于忙碌状态, 返回 1 表示空闲的状态.
 * 检测是否忙碌是通过查看是否还有未执行完的套接字命令或者套接字 I/O 事件.
 *
 * 函数无参数
 * 返回: 0 表示需要退出套接字模块线程; 1 表示空闲的状态; -1 表示套接字模块处于忙碌状态, 不应该打断; */
int 
skynet_socket_poll() {
	struct socket_server *ss = SOCKET_SERVER;
	assert(ss);
	struct socket_message result;
	int more = 1;
	int type = socket_server_poll(ss, &result, &more);
	switch (type) {
	case SOCKET_EXIT:
		return 0;
	case SOCKET_DATA:
		forward_message(SKYNET_SOCKET_TYPE_DATA, false, &result);
		break;
	/* SOCKET_CLOSE 的 result 中的 data 等于 NULL */
	case SOCKET_CLOSE:
		forward_message(SKYNET_SOCKET_TYPE_CLOSE, false, &result);
		break;
	case SOCKET_OPEN:
		forward_message(SKYNET_SOCKET_TYPE_CONNECT, true, &result);
		break;
	case SOCKET_ERROR:
		forward_message(SKYNET_SOCKET_TYPE_ERROR, true, &result);
		break;
	/* SOCKET_ACCEPT 中得到的新的连接还没有 I/O 事件通知, 需要调用 skynet_socket_start 来开启 */
	case SOCKET_ACCEPT:
		forward_message(SKYNET_SOCKET_TYPE_ACCEPT, true, &result);
		break;
	case SOCKET_UDP:
		forward_message(SKYNET_SOCKET_TYPE_UDP, false, &result);
		break;
	default:
		skynet_error(NULL, "Unknown socket message type %d.",type);
		return -1;
	}
	if (more) {
		return -1;
	}
	return 1;
}

/* 检查写缓冲的大小是否超过 1MB 的阈值, 如果超过了将会向服务 ctx 发送一条类型为 SKYNET_SOCKET_TYPE_WARNING 的套接字消息.
 * 其 ud 为写缓冲的大小, 单位是 KB.
 *
 * 参数: ctx 是超过阈值时接收消息的服务; id 是套接字的标识; buffer 参数未使用, 为上次给套接字的数据; wsz 是套接字的写缓冲的大小;
 * 返回: -1 表示检查失败; 0 表示检查成功, 此时可能发送了警告信息, 也可能没有; */
static int
check_wsz(struct skynet_context *ctx, int id, void *buffer, int64_t wsz) {
	if (wsz < 0) {
		return -1;
	} else if (wsz > 1024 * 1024) {
		struct skynet_socket_message tmp;
		tmp.type = SKYNET_SOCKET_TYPE_WARNING;
		tmp.id = id;
		tmp.ud = (int)(wsz / 1024);
		tmp.buffer = NULL;
		skynet_send(ctx, 0, skynet_context_handle(ctx), PTYPE_SOCKET, 0 , &tmp, sizeof(tmp));
//		skynet_error(ctx, "%d Mb bytes on socket %d need to send out", (int)(wsz / (1024 * 1024)), id);
	}
	return 0;
}

/* 服务 ctx 向套接字 id 发送高权限数据 buffer , 发送数据的大小为 sz. 此函数可以针对 TCP 和 UDP 两种套接字.
 * 返回: 检查写缓冲大小的结果, 0 表示成功, -1 表示失败. */
int
skynet_socket_send(struct skynet_context *ctx, int id, void *buffer, int sz) {
	int64_t wsz = socket_server_send(SOCKET_SERVER, id, buffer, sz);
	return check_wsz(ctx, id, buffer, wsz);
}

/* 服务 ctx 向套接字 id 发送地权限数据 buffer , 发送数据的大小为 sz. 此函数可以针对 TCP 和 UDP 两种套接字. */
void
skynet_socket_send_lowpriority(struct skynet_context *ctx, int id, void *buffer, int sz) {
	socket_server_send_lowpriority(SOCKET_SERVER, id, buffer, sz);
}

/* 服务 ctx 侦听地址由主机 host 和端口 port 标识的地址, 其中 host 可为 NULL 或者空字符串, 此时将侦听 0.0.0.0 .
 * backlog 指示未完成连接的请求的队列大小. 成功之后需要调用 skynet_socket_start 函数开启套接字接收连接.
 *
　×　返回: 成功时返回套接字 id, 失败将返回小于 0 的值. */
int 
skynet_socket_listen(struct skynet_context *ctx, const char *host, int port, int backlog) {
	uint32_t source = skynet_context_handle(ctx);
	return socket_server_listen(SOCKET_SERVER, source, host, port, backlog);
}

/* 服务 ctx 连接地址由主机 host 和端口由 port 标识的地址. 其中 host 和 port 必须是有效的地址.
 * 成功将以异步消息通知 SKYNET_SOCKET_TYPE_CONNECT , 失败时将以异步方式通知 SKYNET_SOCKET_TYPE_ERROR .
 *
 * 返回: 调用成功返回套接字的 id , 但不一定此时连接成功, 失败时返回 -1 . */
int 
skynet_socket_connect(struct skynet_context *ctx, const char *host, int port) {
	uint32_t source = skynet_context_handle(ctx);
	return socket_server_connect(SOCKET_SERVER, source, host, port);
}

/* 将操作系统套接字描述符 fd 和服务 ctx 绑定, 从而为所属于服务 ctx .
 * 成功将以异步消息通知 SKYNET_SOCKET_TYPE_CONNECT , 失败时将以异步方式通知 SKYNET_SOCKET_TYPE_ERROR .
 *
 * 返回: 绑定后生成的套接字 id , 失败时将返回 -1 . */
int 
skynet_socket_bind(struct skynet_context *ctx, int fd) {
	uint32_t source = skynet_context_handle(ctx);
	return socket_server_bind(SOCKET_SERVER, source, fd);
}

/* 以更为优雅的方式关闭套接字 id , 被关闭的套接字会等待套接字中所有的写缓冲都被写入完成后才会真正的关闭.
 * 如果发送命令的服务 ctx 不是套接字所属的服务, 可能接收不到异步通知.
 *
 * 此函数没有返回值 */
void 
skynet_socket_close(struct skynet_context *ctx, int id) {
	uint32_t source = skynet_context_handle(ctx);
	socket_server_close(SOCKET_SERVER, source, id);
}

/* 强制关闭套接字 id, 被关闭的套接字将在处理线程接收到命令之后尽可能多的发送写缓冲, 并尽快关闭套接字.
 * 如果发送命令的服务 ctx 不是套接字所属的服务, 可能接收不到异步通知.
 *
 * 此函数没有返回值 */
void 
skynet_socket_shutdown(struct skynet_context *ctx, int id) {
	uint32_t source = skynet_context_handle(ctx);
	socket_server_shutdown(SOCKET_SERVER, source, id);
}

/* 可对类型为 SOCKET_TYPE_PACCEPT, SOCKET_TYPE_PLISTEN 和 SOCKET_TYPE_CONNECTED 的套接字发起启动命令.
 * 首先将导致前两种套接字开始接收 I/O 事件通知, 其次将导致第三种套接字的所属服务变为服务 ctx . 最终将以异步方式通知
 * 服务成功或失败.
 *
 * 函数无返回值 */
void 
skynet_socket_start(struct skynet_context *ctx, int id) {
	uint32_t source = skynet_context_handle(ctx);
	socket_server_start(SOCKET_SERVER, source, id);
}

/* 设置套接字 id 的非延迟属性, 其中服务 ctx 为发起命令的服务, 但在函数中未使用到. */
void
skynet_socket_nodelay(struct skynet_context *ctx, int id) {
	socket_server_nodelay(SOCKET_SERVER, id);
}

/* 异步方式生成一个 UDP 套接字, 如果提供了主机 addr 和端口 port 将把此套接字绑定到此地址上. 如果没有则不绑定,
 * 且其套接字类型为 UDP , 函数可以接收 UDP 和 UDPv6 两种形式的地址, 套接字类型与地址类型一致.
 *
 * 返回: 成功时返回套接字 id, 失败时返回 -1 . */
int 
skynet_socket_udp(struct skynet_context *ctx, const char * addr, int port) {
	uint32_t source = skynet_context_handle(ctx);
	return socket_server_udp(SOCKET_SERVER, source, addr, port);
}

/* 发起 UDP 连接, 其实质是将由主机 addr 和端口 port 标识的地址关联到套接字中去, 需要注意套接字类型要与地址类型一致.
 * 成功之后可以调用 skynet_socket_send 和 skynet_socket_send_lowpriority 发送消息.
 *
 * 返回: 0 表示连接成功, -1 表示连接失败. */
int 
skynet_socket_udp_connect(struct skynet_context *ctx, int id, const char * addr, int port) {
	return socket_server_udp_connect(SOCKET_SERVER, id, addr, port);
}

/* 向指定地址 address 发送高权限 UDP 包. 地址的格式必须是 socket_server 内部定义的地址格式,
 * 这个地址可以从 skynet_socket_udp_address 中取得, 地址可以是 UDP 和 UDPv6 两种形式的.
 *
 * 返回: 检查写缓冲大小的结果, 0 表示成功, -1 表示失败. */
int 
skynet_socket_udp_send(struct skynet_context *ctx, int id, const char * address, const void *buffer, int sz) {
	int64_t wsz = socket_server_udp_send(SOCKET_SERVER, id, (const struct socket_udp_address *)address, buffer, sz);
	return check_wsz(ctx, id, (void *)buffer, wsz);
}

/* 从 skynet_socket_message 中提取出 socket_server 格式的地址, 地址的长度由 addrsz 接收.
 * 返回: 地址内存的起点 */
const char *
skynet_socket_udp_address(struct skynet_socket_message *msg, int *addrsz) {
	if (msg->type != SKYNET_SOCKET_TYPE_UDP) {
		return NULL;
	}
	struct socket_message sm;
	sm.id = msg->id;
	sm.opaque = 0;
	sm.ud = msg->ud;
	sm.data = msg->buffer;
	return (const char *)socket_server_udp_address(SOCKET_SERVER, &sm, addrsz);
}
