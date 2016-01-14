#ifndef skynet_socket_h
#define skynet_socket_h

struct skynet_context;

/* struct skynet_socket_message 中的 type 取值, 标识所有的套接字事件 */
#define SKYNET_SOCKET_TYPE_DATA 1
#define SKYNET_SOCKET_TYPE_CONNECT 2
#define SKYNET_SOCKET_TYPE_CLOSE 3
#define SKYNET_SOCKET_TYPE_ACCEPT 4
#define SKYNET_SOCKET_TYPE_ERROR 5
#define SKYNET_SOCKET_TYPE_UDP 6
#define SKYNET_SOCKET_TYPE_WARNING 7

/* 发送到 skynet 各个服务去的套接字消息 */
struct skynet_socket_message {
	int type;           /* 消息类型, 取值在上边描述 */
	int id;             /* 套接字连接的 id */
	int ud;             /* 当 ACCEPT 时 ud 表示在侦听端口上连接上来的套接字连接的 id, 当为 DATA 时表示
	                       接收到的数据大小, 其它情况下均为 0 */
	char * buffer;      /* 当为 DATA 时表示数据内容, 其它情况下或者为 NULL 或者为错误信息等 */
};

void skynet_socket_init();
void skynet_socket_exit();
void skynet_socket_free();
int skynet_socket_poll();

int skynet_socket_send(struct skynet_context *ctx, int id, void *buffer, int sz);
void skynet_socket_send_lowpriority(struct skynet_context *ctx, int id, void *buffer, int sz);
int skynet_socket_listen(struct skynet_context *ctx, const char *host, int port, int backlog);
int skynet_socket_connect(struct skynet_context *ctx, const char *host, int port);
int skynet_socket_bind(struct skynet_context *ctx, int fd);
void skynet_socket_close(struct skynet_context *ctx, int id);
void skynet_socket_shutdown(struct skynet_context *ctx, int id);
void skynet_socket_start(struct skynet_context *ctx, int id);
void skynet_socket_nodelay(struct skynet_context *ctx, int id);

int skynet_socket_udp(struct skynet_context *ctx, const char * addr, int port);
int skynet_socket_udp_connect(struct skynet_context *ctx, int id, const char * addr, int port);
int skynet_socket_udp_send(struct skynet_context *ctx, int id, const char * address, const void *buffer, int sz);
const char * skynet_socket_udp_address(struct skynet_socket_message *, int *addrsz);

#endif
