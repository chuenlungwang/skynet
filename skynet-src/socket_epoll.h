#ifndef poll_socket_epoll_h
#define poll_socket_epoll_h

#include <netdb.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

/* 此文件时 skynet_poll.h 在 Linux 环境下的实现.
 * 需要注意的是 bool 的类型定义是从 socket_poll.h 引入的. */

/* 检查 I/O 侦听文件描述符是否无效. 有效时 false, 无效时返回 true. */
static bool 
sp_invalid(int efd) {
	return efd == -1;
}

/* 创建一个 I/O 侦听文件描述符.
 * 成功时返回 epoll 文件描述符用于接下来的工作, 失败时返回 -1 并设置相应的 errno.
 * 此函数保证返回的文件描述符至少可以同时侦听 1024 个文件描述符上的 I/O 事件. */
static int
sp_create() {
	return epoll_create(1024);
}

/* 使用完 epoll 后释放 epoll 并关闭侦听文件描述符.
 * 在关闭此文件描述符前, 需要先调用 sp_del 移除正在侦听的文件描述符.
 * 此函数只在使用完 epoll 后调用一次. */
static void
sp_release(int efd) {
	close(efd);
}

/* 添加新的关联文件( socket 或者 pipe 的文件描述符 )到 efd 句柄中, 对关联文件的可读事件进行注册.
 * sock 是关联文件, ud 是与此关联文件的用户数据, 此用户数据在事件发生时访问.
 * 添加成功时返回 0, 失败时返回 1 . */
static int 
sp_add(int efd, int sock, void *ud) {
	struct epoll_event ev;
	ev.events = EPOLLIN;
	ev.data.ptr = ud;
	if (epoll_ctl(efd, EPOLL_CTL_ADD, sock, &ev) == -1) {
		return 1;
	}
	return 0;
}

/* 从 epoll 侦听队列中移除关联文件( socket 或者 pipe 的文件描述符 ), 
 * 移除后将不再侦听关联文件中的 I/O 事件. 参数 efd 为 epoll 文件描述符,
 * sock 为关联文件. 此函数无返回值. */
static void 
sp_del(int efd, int sock) {
	epoll_ctl(efd, EPOLL_CTL_DEL, sock , NULL);
}

/* 打开或关闭关联文件的可写事件侦听, 并同时修改关联文件上的用户数据.
 * 参数 efd 是 epoll 文件描述符, ud 是关联文件上的用户数据, enable 为
 * true 时表示侦听可写事件, false 表示不侦听可写事件. 此函数会保证
 * 关联文件的可读事件保持侦听. 调用此函数前需要将关联文件注册到 epoll 上. */
static void 
sp_write(int efd, int sock, void *ud, bool enable) {
	struct epoll_event ev;
	ev.events = EPOLLIN | (enable ? EPOLLOUT : 0);
	ev.data.ptr = ud;
	epoll_ctl(efd, EPOLL_CTL_MOD, sock, &ev);
}

/* 以阻塞方式等待并接收 epoll 侦听列表上的 I/O 事件.
 * 调用此函数会阻塞直到任一关联文件可以进行 I/O 操作或者被中断.
 * 事件将被填充在参数 e 中, 参数 efd 是 epoll 文件描述符, max 为最大获取事件数量,
 * 必须是正数, 一般为 e 数组的长度. 函数返回的发生 I/O 事件的文件数量. */
static int 
sp_wait(int efd, struct event *e, int max) {
	struct epoll_event ev[max];
	int n = epoll_wait(efd , ev, max, -1);
	int i;
	for (i=0;i<n;i++) {
		/* ev[i].data.ptr 为先前设置的用户数据, 维持不变 */
		e[i].s = ev[i].data.ptr;
		unsigned flag = ev[i].events;
		e[i].write = (flag & EPOLLOUT) != 0;
		e[i].read = (flag & EPOLLIN) != 0;
	}

	return n;
}

/* 将文件描述符所表示的流设置为非阻塞的. */
static void
sp_nonblocking(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	if ( -1 == flag ) {
		return;
	}

	fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

#endif
