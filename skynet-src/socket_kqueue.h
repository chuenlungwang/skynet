#ifndef poll_socket_kqueue_h
#define poll_socket_kqueue_h

#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/event.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* 此文件是 socket_poll.h 在 BSD 接口下的实现 I/O 多路复用, 采用了 kqueue 机制实现方式.
 * 默认实现方式是水平触发模式, 当某一 I/O 事件出现时而应用程序不处理此事件, 就会
 * 一直收到事件通知. 因此, 程序有时会禁止可写事件侦听.
 * 需要注意的是 bool 的类型定义是从 socket_poll.h 引入的. */

/* 校验一个 kqueue 文件描述符是否有效. 有效时 false, 无效时返回 true. */
static bool
sp_invalid(int kfd) {
	return kfd == -1;
}

/* 创建一个 kqueue 内核事件队列, 使用完之后需调用 sp_release 释放此事件队列.
 * 成功时返回 kqueue 文件描述符, 失败时返回 -1 并设置相应的 errno. */
static int
sp_create() {
	return kqueue();
}

/* 使用完 kqueue 后释放 kequeue 并关闭侦听的文件描述符.
 * [ck]在关闭此文件描述符前, 需要先调用 sp_del 移除正在侦听的文件描述符. [/ck]
 * 此函数只在使用完 kqueue 后调用一次. */
static void
sp_release(int kfd) {
	close(kfd);
}

/* 从 kqueue 事件队列中解除关联文件描述符( socket 或者 pipe 的文件描述符 ) 的读写事件通知.
 * 参数 kfd 为 kqueue 的文件描述符, sock 为关联文件描述符. 此函数无返回值.
 * 此函数只应对已经关联了的文件描述符调用一次, 其它情况下调用无效. */
static void 
sp_del(int kfd, int sock) {
	struct kevent ke;
	EV_SET(&ke, sock, EVFILT_READ, EV_DELETE, 0, 0, NULL);
	kevent(kfd, &ke, 1, NULL, 0, NULL);
	EV_SET(&ke, sock, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
	kevent(kfd, &ke, 1, NULL, 0, NULL);
}

/* 将一个文件描述符( socket 或 pipe 的文件描述符 )添加到 kqueue 队列中,
 * 对关联文件描述符的可读事件进行注册.
 * sock 是关联文件的描述符, ud 是与此关联文件描述符相关的用户数据, 此用户数据在事件发生时返回.
 * 添加成功时返回 0, 失败时返回 1 . 当不再关心关联文件描述符的读写事件时需要调用 sp_del 解除关联. */
static int 
sp_add(int kfd, int sock, void *ud) {
	struct kevent ke;
	EV_SET(&ke, sock, EVFILT_READ, EV_ADD, 0, 0, ud);
	if (kevent(kfd, &ke, 1, NULL, 0, NULL) == -1) {
		return 1;
	}
	/* 需要先添加可写事件通知, 并禁用它, 为的是以后直接打开可写事件通知.
	 * 如果任何一步出错, 需要清理之前的动作. */
	EV_SET(&ke, sock, EVFILT_WRITE, EV_ADD, 0, 0, ud);
	if (kevent(kfd, &ke, 1, NULL, 0, NULL) == -1) {
		EV_SET(&ke, sock, EVFILT_READ, EV_DELETE, 0, 0, NULL);
		kevent(kfd, &ke, 1, NULL, 0, NULL);
		return 1;
	}
	EV_SET(&ke, sock, EVFILT_WRITE, EV_DISABLE, 0, 0, ud);
	if (kevent(kfd, &ke, 1, NULL, 0, NULL) == -1) {
		sp_del(kfd, sock);
		return 1;
	}
	return 0;
}

/* 打开或关闭关联文件描述符的可写事件通知, 并同时修改关联文件描述符上的用户数据.
 * 参数 kfd 是 kqueue 的文件描述, sock 是关联文件描述符, ud 是关联文件描述符上的用户数据,
 * enable 为 true 时表示侦听可写事件, false 表示不侦听可写事件. 此函数会保证
 * 关联文件描述符的可读事件保持侦听. 调用此函数需要事先将关联文件描述符添加到内核队列上,
 * 其它情况下调用无效. */
static void 
sp_write(int kfd, int sock, void *ud, bool enable) {
	struct kevent ke;
	EV_SET(&ke, sock, EVFILT_WRITE, enable ? EV_ENABLE : EV_DISABLE, 0, 0, ud);
	if (kevent(kfd, &ke, 1, NULL, 0, NULL) == -1) {
		// todo: check error
	}
}

/* 以阻塞方式等待并接收 kqueue 上的 I/O 事件通知. 调用此函数会一直阻塞直到任一关联
 * 文件描述符可以进行相关的 I/O 操作或者被中断. 事件被填充到参数 e 中, efd 为 kqueue
 * 的文件描述符, max 为最大接收事件数量, 必须是正数, 一般为 e 数组的长度.
 * 函数返回的发生 I/O 事件的文件数量. */
static int 
sp_wait(int kfd, struct event *e, int max) {
	struct kevent ev[max];
	int n = kevent(kfd, NULL, 0, ev, max, NULL);

	/* 此处有一点需要注意, 一个 ev 元素只能表示一个写事件或者读事件, 不能同时表示. */
	int i;
	for (i=0;i<n;i++) {
		e[i].s = ev[i].udata;
		unsigned filter = ev[i].filter;
		e[i].write = (filter == EVFILT_WRITE);
		e[i].read = (filter == EVFILT_READ);
	}

	return n;
}

/* 将文件描述符所表示的流设置为非阻塞的, 不改变其它状态值. */
static void
sp_nonblocking(int fd) {
	int flag = fcntl(fd, F_GETFL, 0);
	if ( -1 == flag ) {
		return;
	}

	fcntl(fd, F_SETFL, flag | O_NONBLOCK);
}

#endif
