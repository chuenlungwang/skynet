#ifndef socket_poll_h
#define socket_poll_h

#include <stdbool.h>

/* I/O 多路复用的文件描述符, 实现为在 Linux 为 epoll 文件描述符,
 * 在 BSD 接口下为 kqueue 文件描述符. */
typedef int poll_fd;

/* 接收到的 I/O 事件通知 */
struct event {
	void * s;      /* 事件通知中的用户数据, 与注册时添加的用户数据一模一样. */
	bool read;     /* 可读事件标记 */
	bool write;    /* 可写事件标记 */
};

static bool sp_invalid(poll_fd fd);
static poll_fd sp_create();
static void sp_release(poll_fd fd);
static int sp_add(poll_fd fd, int sock, void *ud);
static void sp_del(poll_fd fd, int sock);
static void sp_write(poll_fd, int sock, void *ud, bool enable);
static int sp_wait(poll_fd, struct event *e, int max);
static void sp_nonblocking(int sock);

#ifdef __linux__
#include "socket_epoll.h"
#endif

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined (__NetBSD__)
#include "socket_kqueue.h"
#endif

#endif
