#include "skynet.h"

#include "socket_server.h"
#include "socket_poll.h"
#include "atomic.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <string.h>

#define MAX_INFO 128          /* 短消息的最大长度 */
// MAX_SOCKET will be 2^MAX_SOCKET_P
#define MAX_SOCKET_P 16
#define MAX_EVENT 64          /* I/O 多路复用时一次性侦听的最大事件数量 */
#define MIN_READ_BUFFER 64    /* 从套接字中一次性最少读取的字节数 */

/* socket 的状态类型, 保存在 socket 结构对象中 */
#define SOCKET_TYPE_INVALID 0      /* 套接字连接对象不可用或损坏, 同时也表示套接字对象未被使用 */
#define SOCKET_TYPE_RESERVE 1      /* 套接字连接对象已经被分配了, 但还没有实际进行网络连接 */
#define SOCKET_TYPE_PLISTEN 2      /* 已经在侦听端口, 但是不上报事件, 当调用 start_socket 才变成 LISTEN */
#define SOCKET_TYPE_LISTEN 3       /* 已经在侦听端口, 并且可以上报事件 */
#define SOCKET_TYPE_CONNECTING 4   /* 套接字正在连接, 但是还没有连接上, 此时还不能传送信息 */
#define SOCKET_TYPE_CONNECTED 5    /* 套接字连接成功, 可以发送信息 */
#define SOCKET_TYPE_HALFCLOSE 6    /* 半关闭状态, 虽然套接字本身没有关闭, 但是已经不能往里边添加信息了, 最终会在清空写缓冲的情况下关闭 */
#define SOCKET_TYPE_PACCEPT 7      /* 已经接受了客户端的连接, 但是不上报数据, 当调用 start_socket 才变成 CONNECTED */
#define SOCKET_TYPE_BIND 8         /* 将套接字描述符绑定到 skynet 系统的套接字上, 此套接字描述由别处生成 */

/* 最大套接字数量为 2^16 个 */
#define MAX_SOCKET (1<<MAX_SOCKET_P)

#define PRIORITY_HIGH 0
#define PRIORITY_LOW 1

#define HASH_ID(id) (((unsigned)id) % MAX_SOCKET)

/* 支持三种协议类型 TCP UDP UDPv6 */
#define PROTOCOL_TCP 0
#define PROTOCOL_UDP 1
#define PROTOCOL_UDPv6 2

#define UDP_ADDRESS_SIZE 19	// ipv6 128bit + port 16bit + 1 byte type

#define MAX_UDP_PACKAGE 65535

// EAGAIN and EWOULDBLOCK may be not the same value.
#if (EAGAIN != EWOULDBLOCK)
#define AGAIN_WOULDBLOCK EAGAIN : case EWOULDBLOCK
#else
#define AGAIN_WOULDBLOCK EAGAIN
#endif

/* socket 写入的缓存数据, 如果是 TCP 协议将不包含 udp_address 字段, 而仅仅是前面部分 */
struct write_buffer {
	struct write_buffer * next;               /* 处于 wb_list 中的下一个写缓存 */
	void *buffer;                             /* 调用者传递过来的缓存, 从中可以提取出发送数据, 最后需要回收内存 */
	char *ptr;                                /* 发送数据的起始指针, 会随着不断写入 socket 而向后移动 */
	int sz;                                   /* 发送数据的大小, 会随着不断写入 socket 而减小 */
	bool userobject;                          /* 是否使用用户对象, 如果使用的话, 将调用 socket_object_interface 中的函数释放 buffer 字段指向的内存 */
	uint8_t udp_address[UDP_ADDRESS_SIZE];    /* 保存 udp 的地址, 头字节是协议类型, 可能为 UDP 或者 UDPv6 两种, 接下来是两个字节是端口号, 剩下的为 ip ,
	                                           * ip 可能是 ipv4(32bit) 也可能是 ipv6(128bit) */
};

/* TCP 的写入缓存大小不包括 udp_address 部分而 UDP 写入缓存会包括 */
#define SIZEOF_TCPBUFFER (offsetof(struct write_buffer, udp_address[0]))
#define SIZEOF_UDPBUFFER (sizeof(struct write_buffer))

/* 套接字写入缓存数据的列表, 初始时 head 和 tail 均为 NULL */
struct wb_list {
	struct write_buffer * head;
	struct write_buffer * tail;
};

/* 表示一个 socket 连接的对象 */
struct socket {
	uintptr_t opaque;          /* 不透明对象对象, 为连接所属的服务的地址 */
	struct wb_list high;       /* 优先级更高的写入缓存数据队列 */
	struct wb_list low;        /* 优先级较低的写入缓存数据队列 */
	int64_t wb_size;           /* 写入缓存的大小, 会随着添加写入缓存而增大, 同时随着写入成功而减小 */
	int fd;                    /* 套接字连接对象的网络连接的文件描述符 */
	int id;                    /* 套接字连接对象的唯一 id */
	uint16_t protocol;         /* 支持的协议, 为 TCP UDP UDPv6 中的一种 */
	uint16_t type;             /* 套接字连接对象的状态类型, 为上面描述的 9 种类型之一 */
	union {
		int size;              /* 在 TCP 协议下使用, 表示一次性读取的字节数 */
		uint8_t udp_address[UDP_ADDRESS_SIZE];
		                       /* 在 UDP UDPv6 协议下使用, 表示对端 ip 地址 */
	} p;
};

/* 套接字服务器对象 */
struct socket_server {
	int recvctrl_fd;                         /* 接收命令的管道的文件描述符 */
	int sendctrl_fd;                         /* 发送命令的管道的文件描述符 */
	int checkctrl;                           /* 是否需要检查管道中的命令的标记 */
	poll_fd event_fd;                        /* 多路 I/O 事件的文件描述符 */
	int alloc_id;                            /* 分配套接字对象唯一 id 的起点 */
	int event_n;                             /* 本次接收到的 I/O 事件通知数量 */
	int event_index;                         /* 此时处理到的 I/O 事件通知的索引, 值保存在 ev 字段中, 会随着处理而递增 */
	struct socket_object_interface soi;      /* 自定义的提取写入缓存和销毁缓存函数接口 */
	struct event ev[MAX_EVENT];              /* 接收多路 I/O 事件通知的事件对象, 具体参见 socket_poll.h 文件 */
	struct socket slot[MAX_SOCKET];          /* 保存所有套接字对象的插槽 */
	char buffer[MAX_INFO];                   /* 用于保存一些较短的信息, 这些信息绝多数是字符串形式的 ip 地址 */
	uint8_t udpbuffer[MAX_UDP_PACKAGE];      /* 用于接收 udp 协议发送过来的消息内容 */
	fd_set rfds;                             /* 接收命令的管道的 select 监控文件描述符集 */
};

/* 发起到一个 ip 和端口连接的请求体 */
struct request_open {
	int id;                 /* 即将打开的连接套接字 id */
	int port;               /* 对端的端口号 */
	uintptr_t opaque;       /* 携带的用户数据, 在此项目中为 skynet 服务的 id */
	char host[1];           /* 对端的 ip 地址字符串, 之所以大小只有 1 是因为此结构将包含在 request_package 中的联合 u 中,
	                           那里有足够的连续内存 */
};

/* TCP 发送字节信息的请求体 */
struct request_send {
	int id;                 /* 发送此消息的套接字 id */
	int sz;                 /* 消息内容的大小 */
	char * buffer;          /* 消息的数据内容 */
};

/* UDP 发送字节信息的请求体 */
struct request_send_udp {
	struct request_send send;           /* 发送的消息内容, 同 TCP 发送字节消息请求体 */
	uint8_t address[UDP_ADDRESS_SIZE];  /* 对端地址, 格式是协议类型+端口号+ IPv4 或 IPv6 的二进制格式 */
};

/* 给一个套接字设置 UDP 地址的请求体 */
struct request_setudp {
	int id;                             /* 需要设置 UDP 地址的套接字 id */
	uint8_t address[UDP_ADDRESS_SIZE];  /* 设置在 UDP 无连接套接字中对端地址, 格式为协议+端口号+ IPv4 或 IPv6 的二进制格式 */
};

/* 关闭一个套接字的请求体 */
struct request_close {
	int id;                /* 待关闭的套接字 id */
	int shutdown;          /* 是否立即关闭 */
	uintptr_t opaque;      /* 不透明对象, 此项目中为一个 skynet 服务地址 */
};

/* 启动端口侦听的请求体, 整个逻辑过程是先已经得到侦听某个端口的套接字描述符 fd , 然后再将其
 * 与套接字 id 关联起来, 最后启动此套接字开始上报连接信息 */
struct request_listen {
	int id;                /* 需要关联已经侦听的套接字描述符的套接字 id */
	int fd;                /* 待关联的已经侦听端口的系统套接字描述符 */
	uintptr_t opaque;      /* 不透明对象, 此项目中为此套接字所属的 skynet 服务地址 */
	char host[1];          /* 未使用 */
};

/* 将系统套接字描述符关联到套接字上 */
struct request_bind {
	int id;                /* 待关联的套接字 id */
	int fd;                /* 系统中的套接字描述符 */
	uintptr_t opaque;      /* 不透明对象, 此项目中为此套接字所属的 skynet 服务地址 */
};

/* 开启 PLISTEN 和 PACCEPT 类型的套接字调用 start_sockt 函数, 开启数据上报的请求体 */
struct request_start {
	int id;                /* 类型必须为 PLISTEN 和 PACCEPT 的套接字 */
	uintptr_t opaque;      /* 不透明对象, 此项目中为此套接字所属的 skynet 服务地址 */
};

/* 设置套接字选项的请求体 */
struct request_setopt {
	int id;                /* 套接字的 id */
	int what;              /* 选项的键 */
	int value;             /* 选项的值 */
};

/* 将一个 UDP 的系统套接字描述符关联到套接字 id 上的请求体. 这类似于 TCP 的侦听操作, 其逻辑是
 * 先将一个 UDP 系统套接字描述符绑定到端口上, 并将它与套接字 id 关联起来. */
struct request_udp {
	int id;                /* skynet 中的套接字 id */
	int fd;                /* 关联到套接字上的系统 UDP 套接字描述符, 此描述符已经绑定到了一个端口上了 */
	int family;            /* 套接字描述符所属的地址族, 目前只支持 AF_INET 和 AF_INET6 */
	uintptr_t opaque;      /* 不透明对象, 此项目中为此套接字所属的 skynet 服务地址 */
};

/*
	The first byte is TYPE

	S Start socket
	B Bind socket
	L Listen socket
	K Close socket
	O Connect to (Open)
	X Exit
	D Send package (high)
	P Send package (low)
	A Send UDP package
	T Set opt
	U Create UDP socket
	C set udp address
 */

/* socket_server 发送各种套接字的命令与实际的执行并不是在一条线程中, 发送套接字命令的线程可以是任意的,
 * 即便此时处理线程尚未开始工作或者已经关闭了也是可以发送命令的. 它们之间通过一个管道衔接起来, 从而构成一个
 * 异步的系统, 处理线程的主循环是 socket_server_poll 函数. */

struct request_package {
	uint8_t header[8];	// 6 bytes dummy     其第 7 和第 8 字节用来表示请求的类型和请求体大小
	union {                                  /* 请求体所在位置, 大小有 256 个字节 */
		char buffer[256];
		struct request_open open;             /* 发起 TCP 连接 */
		struct request_send send;             /* 发送 TCP 流数据 */
		struct request_send_udp send_udp;     /* 发送 UDP 数据包 */
		struct request_close close;           /* 关闭套接字 */
		struct request_listen listen;         /* 侦听端口 */
		struct request_bind bind;             /* 将系统套接字描述符关联到套接字上 */
		struct request_start start;           /* 对 PACCEPT 和 PLISTEN 类型的套接字开启数据上报 */
		struct request_setopt setopt;         /* 设置套接字的选项 */
		struct request_udp udp;               /* 生成一个 UDP 套接字 */
		struct request_setudp set_udp;        /* 给 UDP 套接字设置对端地址 */
	} u;
	uint8_t dummy[256];                      /* 更多的 256 个字节, 在上面不够时使用 */
};

/* 各种类型的套接字地址 */
union sockaddr_all {
	struct sockaddr s;          /* 各种 socket 操作传入的 sock 地址 */
	struct sockaddr_in v4;      /* ipv4 地址的结构定义 */
	struct sockaddr_in6 v6;     /* ipv6 地址的结构定义 */
};

/* 发送数据的结构, 内包含释放内存函数指针 */
struct send_object {
	void * buffer;                /* 数据缓存的起始指针 */
	int sz;                       /* 数据缓存的大小 */
	void (*free_func)(void *);    /* 释放缓存的函数, 释放的是 request 中的缓存, 而不是此结构中的缓存, 仅当在 wb_list 外部写入数据时才调用此函数 */
};

#define MALLOC skynet_malloc
#define FREE skynet_free

/* 初始化发送对象, 当大小 sz 小于 0 时, 将调用 socket_server 中的 soi 函数接口从 object 中提取发送对象.
 * 否则直接 object 为发送缓存, sz 为缓存的大小.
 *
 * 参数: ss 是套接字服务器, so 是发送对象, object 是发送缓存, 如果 sz 小于 0 时将从中提取发送缓存,
 *      sz 是发送缓存大小, 可以小于 0 .
 *
 * 返回: 是否调用了 soi 接口的标记 */
static inline bool
send_object_init(struct socket_server *ss, struct send_object *so, void *object, int sz) {
	if (sz < 0) {
		so->buffer = ss->soi.buffer(object);
		so->sz = ss->soi.size(object);
		so->free_func = ss->soi.free;
		return true;
	} else {
		so->buffer = object;
		so->sz = sz;
		so->free_func = FREE;
		return false;
	}
}

/* 释放写入缓存的内存. 如果写入缓存是使用 soi 函数接口得到的数据, 那么最终需要调用其 free 函数释放缓存内容.
 * 参数: ss 是套接字服务器对象, wb 是写入缓存对象
 * 函数没有返回值 */
static inline void
write_buffer_free(struct socket_server *ss, struct write_buffer *wb) {
	if (wb->userobject) {
		ss->soi.free(wb->buffer);
	} else {
		FREE(wb->buffer);
	}
	FREE(wb);
}

/* 设置一个套接字连接为保持连接状态, 传入的参数 fd 为套接字连接的文件描述符. */
static void
socket_keepalive(int fd) {
	int keepalive = 1;
	setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void *)&keepalive , sizeof(keepalive));  
}

/* 从套接字服务器中获取一个唯一的套接字对象 id, id 的生成规则是以原子方式自增整数 alloc_id , 并且当超出最大正整数 0x7fffffff
 * 时回绕至 0 . 得到的 id 再以哈希方式到插槽中去检索空闲槽, 如果找空闲槽则已原子方式占有它, 然后返回此 id .
 * 若最终无法找到空闲槽则返回 -1 .
 *
 * 参数: ss 是套接字服务器
 * 返回: 唯一的套接字对象 id 或者在未获取到时返回 -1 */
static int
reserve_id(struct socket_server *ss) {
	int i;
	for (i=0;i<MAX_SOCKET;i++) {
		/* 如果超过了最大正整数将会回绕到 0 . */
		int id = ATOM_INC(&(ss->alloc_id));
		if (id < 0) {
			id = ATOM_AND(&(ss->alloc_id), 0x7fffffff);
		}
		struct socket *s = &ss->slot[HASH_ID(id)];
		if (s->type == SOCKET_TYPE_INVALID) {
			if (ATOM_CAS(&s->type, SOCKET_TYPE_INVALID, SOCKET_TYPE_RESERVE)) {
				s->id = id;
				s->fd = -1;
				return id;
			} else {
				// retry
				--i;
			}
		}
	}
	return -1;
}

/* 清空套接字写入缓存数据的列表 */
static inline void
clear_wb_list(struct wb_list *list) {
	list->head = NULL;
	list->tail = NULL;
}

/* 在初始化 socket_server 模块时创建套接字服务器对象, 创建命令管道, 并将接收端添加到 I/O 事件通知列表中.
 * 返回: 成功时返回套接字服务器对象, 失败时返回 NULL. */
struct socket_server * 
socket_server_create() {
	int i;
	int fd[2];
	poll_fd efd = sp_create();
	if (sp_invalid(efd)) {
		fprintf(stderr, "socket-server: create event pool failed.\n");
		return NULL;
	}
	if (pipe(fd)) {
		sp_release(efd);
		fprintf(stderr, "socket-server: create socket pair failed.\n");
		return NULL;
	}
	/* 将接收命令端的文件描述符添加到 I/O 事件通知列表中, 当任何一步失败时都需要依次清理文件描述符,
	 * 添加到 I/O 事件的原因在于可以不必时时检测管道中的命令, 而仅当 sp_wait 显示有事件时才去检测. */
	if (sp_add(efd, fd[0], NULL)) {
		// add recvctrl_fd to event poll
		fprintf(stderr, "socket-server: can't add server fd to event pool.\n");
		close(fd[0]);
		close(fd[1]);
		sp_release(efd);
		return NULL;
	}

	struct socket_server *ss = MALLOC(sizeof(*ss));
	ss->event_fd = efd;
	ss->recvctrl_fd = fd[0];
	ss->sendctrl_fd = fd[1];
	ss->checkctrl = 1;

	/* 初始化套接字对象, 分配套接字起始点, 目前事件数量以及处理到的事件索引 */
	for (i=0;i<MAX_SOCKET;i++) {
		struct socket *s = &ss->slot[i];
		s->type = SOCKET_TYPE_INVALID;
		clear_wb_list(&s->high);
		clear_wb_list(&s->low);
	}
	ss->alloc_id = 0;
	ss->event_n = 0;
	ss->event_index = 0;
	memset(&ss->soi, 0, sizeof(ss->soi));
	FD_ZERO(&ss->rfds);
	assert(ss->recvctrl_fd < FD_SETSIZE);

	return ss;
}

/* 释放掉整个写缓存队列, 并将队列重新置为原始状态 */
static void
free_wb_list(struct socket_server *ss, struct wb_list *list) {
	struct write_buffer *wb = list->head;
	while (wb) {
		struct write_buffer *tmp = wb;
		wb = wb->next;
		write_buffer_free(ss, tmp);
	}
	list->head = NULL;
	list->tail = NULL;
}

/* 关闭套接字, 并释放其写入缓存队列的内存. 出参 result 会返回相应的套接字 id 和所属服务句柄.
 * 参数: ss 是套接字服务器; s 是目前需要关闭的套接字; result 为出参;
 * 返回: 此函数无返回值, 真正的返回值在出参中 */
static void
force_close(struct socket_server *ss, struct socket *s, struct socket_message *result) {
	result->id = s->id;
	result->ud = 0;
	result->data = NULL;
	result->opaque = s->opaque;
	/* 若本身已经是未分配的, 将不执行任何操作 */
	if (s->type == SOCKET_TYPE_INVALID) {
		return;
	}
	/* 校验不是刚分配而没有实际使用的套接字 */
	assert(s->type != SOCKET_TYPE_RESERVE);
	free_wb_list(ss,&s->high);
	free_wb_list(ss,&s->low);
	/* 类型为 SOCKET_TYPE_PACCEPT 和 SOCKET_TYPE_PLISTEN 的套接字还没有加入 I/O 事件通知列表中 */
	if (s->type != SOCKET_TYPE_PACCEPT && s->type != SOCKET_TYPE_PLISTEN) {
		sp_del(ss->event_fd, s->fd);
	}
	/* 类型为 SOCKET_TYPE_BIND 的套接字中的系统套接字描述符不是由此模块生成的, 因而不需要关闭 */
	if (s->type != SOCKET_TYPE_BIND) {
		if (close(s->fd) < 0) {
			perror("close socket:");
		}
	}
	s->type = SOCKET_TYPE_INVALID;
}

/* 关闭整个套接字服务器, 并且将释放所有的相关组件. */
void 
socket_server_release(struct socket_server *ss) {
	int i;
	struct socket_message dummy;
	for (i=0;i<MAX_SOCKET;i++) {
		struct socket *s = &ss->slot[i];
		if (s->type != SOCKET_TYPE_RESERVE) {
			force_close(ss, s , &dummy);
		}
	}
	close(ss->sendctrl_fd);
	close(ss->recvctrl_fd);
	sp_release(ss->event_fd);
	FREE(ss);
}

/* 校验是否写入缓存队列是空的 */
static inline void
check_wb_list(struct wb_list *s) {
	assert(s->head == NULL);
	assert(s->tail == NULL);
}

/* 创建一个新的套接字对象. 调用 new_fd 函数前一定是先调用了 reserve_id 函数.
 *
 * 参数: ss 是套接字服务器; id 是新生成的套接字的 id; protocol 是网络协议, 目前支持 PROTOCOL_TCP 、 PROTOCOL_UDP 和 PROTOCOL_UDPv6 三种;
 * opaque 是不透明对象, 通常为一个 skynet 服务地址, skynet 中表示为服务的句柄; add 表示是否添加到 I/O 事件队列中去;
 *
 * 返回: 新的套接字对象 */
static struct socket *
new_fd(struct socket_server *ss, int id, int fd, int protocol, uintptr_t opaque, bool add) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	/* 校验之前已经预先分配到了套接字 id */
	assert(s->type == SOCKET_TYPE_RESERVE);

	if (add) {
		if (sp_add(ss->event_fd, fd, s)) {
			s->type = SOCKET_TYPE_INVALID;
			return NULL;
		}
	}

	s->id = id;
	s->fd = fd;
	s->protocol = protocol;
	s->p.size = MIN_READ_BUFFER;
	s->opaque = opaque;
	s->wb_size = 0;
	check_wb_list(&s->high);
	check_wb_list(&s->low);
	return s;
}

// return -1 when connecting
/* 打开一个 TCP 连接, 并返回操作的状态. 出参 result 的 data 字段将包含错误信息或者对端 ip 地址.
 * 参数: ss 是套接字服务器; request 是打开 TCP 连接的参数, 将包含主机和端口号; 出参 result 包含返回的信息;
 * 返回: SOCKET_ERROR 表示连接失败, SOCKET_OPEN 表示连接成功, -1 表示正在连接 */
static int
open_socket(struct socket_server *ss, struct request_open * request, struct socket_message *result) {
	int id = request->id;
	result->opaque = request->opaque;
	result->id = id;
	result->ud = 0;
	result->data = NULL;
	struct socket *ns;
	int status;
	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	struct addrinfo *ai_ptr = NULL;
	char port[16];
	sprintf(port, "%d", request->port);
	memset(&ai_hints, 0, sizeof( ai_hints ) );
	/* AF_UNSPEC 表示未指定地址族, 可以是 AF_INET 或者 AF_INET6 两种形式,
	 * 并且指定套接字类型和协议为 TCP 的. */
	ai_hints.ai_family = AF_UNSPEC;
	ai_hints.ai_socktype = SOCK_STREAM;
	ai_hints.ai_protocol = IPPROTO_TCP;

	status = getaddrinfo( request->host, port, &ai_hints, &ai_list );
	if ( status != 0 ) {
		result->data = (void *)gai_strerror(status);
		goto _failed;
	}

	int sock= -1;
	for (ai_ptr = ai_list; ai_ptr != NULL; ai_ptr = ai_ptr->ai_next ) {
		sock = socket( ai_ptr->ai_family, ai_ptr->ai_socktype, ai_ptr->ai_protocol );
		if ( sock < 0 ) {
			continue;
		}
		socket_keepalive(sock);
		sp_nonblocking(sock);
		status = connect( sock, ai_ptr->ai_addr, ai_ptr->ai_addrlen);
		/* 有可能连接返回时还有完成连接操作, errno 是 EINPROGRESS */
		if ( status != 0 && errno != EINPROGRESS) {
			close(sock);
			sock = -1;
			continue;
		}
		break;
	}

	if (sock < 0) {
		result->data = strerror(errno);
		goto _failed;
	}

	ns = new_fd(ss, id, sock, PROTOCOL_TCP, request->opaque, true);
	if (ns == NULL) {
		close(sock);
		result->data = "reach skynet socket number limit";
		goto _failed;
	}

	if(status == 0) {
		ns->type = SOCKET_TYPE_CONNECTED;
		/* 获取到对端的地址, 并且将其转化为字符串形式, 返回到 result 的 data 字段中 */
		struct sockaddr * addr = ai_ptr->ai_addr;
		void * sin_addr = (ai_ptr->ai_family == AF_INET) ? (void*)&((struct sockaddr_in *)addr)->sin_addr : (void*)&((struct sockaddr_in6 *)addr)->sin6_addr;
		if (inet_ntop(ai_ptr->ai_family, sin_addr, ss->buffer, sizeof(ss->buffer))) {
			result->data = ss->buffer;
		}
		freeaddrinfo( ai_list );
		return SOCKET_OPEN;
	} else {
		ns->type = SOCKET_TYPE_CONNECTING;
		/* 对正在连接的套接字添加可写 I/O 事件侦听, 因为处于正在连接的状态下, 发送数据会被放到写入缓冲中去,
		 * 添加写事件, 能够在第一时间发送缓冲中的数据, 同时更为重要的是连接成功时套接字会变为可写状态 */
		sp_write(ss->event_fd, ns->fd, ns, true);
	}

	freeaddrinfo( ai_list );
	return -1;
_failed:
	freeaddrinfo( ai_list );
	ss->slot[HASH_ID(id)].type = SOCKET_TYPE_INVALID;
	return SOCKET_ERROR;
}

/* 发送 TCP 套接字中的写入缓冲数据, 当写入失败的情况下会关闭套接字 s , 关闭的结果填入出参 result, 函数返回 SOCKET_CLOSE.
 * 在写入成功的情况下返回值是 -1 , 但这并不表示队列中的内容全部都写完了, 当内核的写缓冲被写满的情况下也会返回 -1.
 *
 * 参数: ss 是套接字服务器, s 是需要写数据的套接字, list 是写缓冲, 出参 result 仅当返回值为 SOCKET_CLOSE 的情况下会返回.
 * 返回: -1 表示写入成功, SOCKET_CLOSE 表示写入失败并且套接字被关闭 */
static int
send_list_tcp(struct socket_server *ss, struct socket *s, struct wb_list *list, struct socket_message *result) {
	while (list->head) {
		struct write_buffer * tmp = list->head;
		for (;;) {
			int sz = write(s->fd, tmp->ptr, tmp->sz);
			if (sz < 0) {
				/* 只有在被信号中断的情况重新写入, 只有在内核写缓冲被写满的情况返回成功,
				 * 其它 errno 均表示写入失败. */
				switch(errno) {
				case EINTR:
					continue;
				case AGAIN_WOULDBLOCK:
					return -1;
				}
				force_close(ss,s, result);
				return SOCKET_CLOSE;
			}
			s->wb_size -= sz;
			/* 写入的大小小于缓冲的大小, 说明内核写缓冲已经被写满了, 此时将不再发送数据 */
			if (sz != tmp->sz) {
				tmp->ptr += sz;
				tmp->sz -= sz;
				return -1;
			}
			break;
		}
		list->head = tmp->next;
		write_buffer_free(ss,tmp);
	}
	list->tail = NULL;

	return -1;
}

/* 从 udp_address 中提取出套接字地址, 此函数支持 PROTOCOL_UDP 和 PROTOCOL_UDPv6 两种形式的地址.
 * 头字节是协议类型, 可能为 UDP 或者 UDPv6 两种, 接下来是两个字节是端口号, 剩下的为 ip , ip 可能是
 * ipv4(32bit) 也可能是 ipv6(128bit).
 *
 * 参数: s 是套接字, 用于检查地址中的协议类型是否匹配; udp_address 保存了网络地址; 出参 sa 联合了三种网络地址格式;
 * 返回: 转化成标准形式后的地址长度, 如果转换不成功将返回 0 .  */
static socklen_t
udp_socket_address(struct socket *s, const uint8_t udp_address[UDP_ADDRESS_SIZE], union sockaddr_all *sa) {
	int type = (uint8_t)udp_address[0];
	if (type != s->protocol)
		return 0;
	uint16_t port = 0;
	memcpy(&port, udp_address+1, sizeof(uint16_t));
	switch (s->protocol) {
	case PROTOCOL_UDP:
		memset(&sa->v4, 0, sizeof(sa->v4));
		sa->s.sa_family = AF_INET;
		sa->v4.sin_port = port;
		memcpy(&sa->v4.sin_addr, udp_address + 1 + sizeof(uint16_t), sizeof(sa->v4.sin_addr));	// ipv4 address is 32 bits
		return sizeof(sa->v4);
	case PROTOCOL_UDPv6:
		memset(&sa->v6, 0, sizeof(sa->v6));
		sa->s.sa_family = AF_INET6;
		sa->v6.sin6_port = port;
		memcpy(&sa->v6.sin6_addr, udp_address + 1 + sizeof(uint16_t), sizeof(sa->v6.sin6_addr)); // ipv6 address is 128 bits
		return sizeof(sa->v6);
	}
	return 0;
}

/* 发送 UDP 套接字中的写入缓冲数据, 当写入失败的情况下不会关闭套接字, 此函数每次发送一个写缓冲节点, 并且不检查是否发送的字节数 .
 * 参数: ss 是套接字服务器; s 是写入缓冲所属的套接字; list 是写入缓冲队列; 出参 result 未使用;
 * 返回: 不论是成功还是失败, 此函数均返回 -1 . */
static int
send_list_udp(struct socket_server *ss, struct socket *s, struct wb_list *list, struct socket_message *result) {
	while (list->head) {
		struct write_buffer * tmp = list->head;
		union sockaddr_all sa;
		socklen_t sasz = udp_socket_address(s, tmp->udp_address, &sa);
		int err = sendto(s->fd, tmp->ptr, tmp->sz, 0, &sa.s, sasz);
		if (err < 0) {
			switch(errno) {
			case EINTR:
			case AGAIN_WOULDBLOCK:
				return -1;
			}
			fprintf(stderr, "socket-server : udp (%d) sendto error %s.\n",s->id, strerror(errno));
			return -1;
/*			// ignore udp sendto error
			
			result->opaque = s->opaque;
			result->id = s->id;
			result->ud = 0;
			result->data = NULL;

			return SOCKET_ERROR;
*/
		}

		s->wb_size -= tmp->sz;
		list->head = tmp->next;
		write_buffer_free(ss,tmp);
	}
	list->tail = NULL;

	return -1;
}

/* 发送写缓冲中的所有数据, 此函数能够处理 TCP 和 UDP 两种协议的写缓冲. 在 TCP 协议下如果发送失败将导致套接字关闭.
 * 参数: ss 是套接字服务器; s 是发送数据的套接字; list 是写缓冲队列; 出参 result 接收套接字关闭结果;
 * 返回: -1 表示成功发送, SOCKET_CLOSE 表示写入失败并且套接字被关闭  */
static int
send_list(struct socket_server *ss, struct socket *s, struct wb_list *list, struct socket_message *result) {
	if (s->protocol == PROTOCOL_TCP) {
		return send_list_tcp(ss, s, list, result);
	} else {
		return send_list_udp(ss, s, list, result);
	}
}

/* 判断写缓冲队列其头结点是否只写入了一部分. 空的写缓冲队列被认为是完全写入的.
 * 判断的方法是当前的写入起点是否等于原始缓冲的起点地址. */
static inline int
list_uncomplete(struct wb_list *s) {
	struct write_buffer *wb = s->head;
	if (wb == NULL)
		return 0;
	
	return (void *)wb->ptr != wb->buffer;
}

/* 将权限低的写缓冲队列中的头结点放到空的高权限的写缓冲队列的头结点. */
static void
raise_uncomplete(struct socket * s) {
	struct wb_list *low = &s->low;
	struct write_buffer *tmp = low->head;
	low->head = tmp->next;
	if (low->head == NULL) {
		low->tail = NULL;
	}

	// move head of low list (tmp) to the empty high list
	struct wb_list *high = &s->high;
	assert(high->head == NULL);

	tmp->next = NULL;
	high->head = high->tail = tmp;
}

/*
 *  Each socket has two write buffer list, high priority and low priority.

	1. send high list as far as possible.
	2. If high list is empty, try to send low list.
	3. If low list head is uncomplete (send a part before), move the head of low list to empty high list (call raise_uncomplete) .
	4. If two lists are both empty, turn off the event. (call check_close)
 */

/* 发送套接字中的写缓冲, 首先函数会发送高权限队列中数据, 当这个队列变为空的情况下将发送低权限缓冲队列中的数据.
 * 如果低权限写缓冲队列中的一个结点没有被完全发送出去, 将移动到空的高权限队列的头结点中去. 如果在未发送低权限写缓冲
 * 队列之前此队列已经是空的, 那么将关闭可写事件的侦听.
 *
 * 参数: ss 是套接字服务器; s 是需要发送数据的套接字; 出参 result 用于接收套接字关闭的结果;
 * 返回: -1 表示正确写入了, SOCKET_CLOSE 表示写入错误, 最终导致套接字关闭 */
static int
send_buffer(struct socket_server *ss, struct socket *s, struct socket_message *result) {
	assert(!list_uncomplete(&s->low));
	// step 1
	if (send_list(ss,s,&s->high,result) == SOCKET_CLOSE) {
		return SOCKET_CLOSE;
	}
	if (s->high.head == NULL) {
		// step 2
		if (s->low.head != NULL) {
			if (send_list(ss,s,&s->low,result) == SOCKET_CLOSE) {
				return SOCKET_CLOSE;
			}
			// step 3
			if (list_uncomplete(&s->low)) {
				raise_uncomplete(s);
			}
		} else {
			// step 4
			/* 在低权限写缓冲发送之前, 高低权限的写缓冲队列都是空的, 将可写事件的侦听关闭,
			 * 并关闭处于半关闭状态的套接字 */
			sp_write(ss->event_fd, s->fd, s, false);

			if (s->type == SOCKET_TYPE_HALFCLOSE) {
				force_close(ss, s, result);
				return SOCKET_CLOSE;
			}
		}
	}

	return -1;
}

/* 将发送数据添加到缓冲队列中去, 函数将先构建一个写缓冲再添加到队列中去. request 中包含了发送数据,
 * 对于不同类型的发送数据用 size 表示不同写缓冲的大小, n 表示从原始发送数据中第几个字节开始是还没有发送的数据.
 *
 * 参数: ss 是套接字服务器; s 是缓冲队列; request 是发送数据; size 是写缓冲的大小; n 是待发送数据的起始字节;
 * 返回: 构建好的当前发送数据的缓冲 */
static struct write_buffer *
append_sendbuffer_(struct socket_server *ss, struct wb_list *s, struct request_send * request, int size, int n) {
	struct write_buffer * buf = MALLOC(size);
	struct send_object so;
	buf->userobject = send_object_init(ss, &so, request->buffer, request->sz);
	buf->ptr = (char*)so.buffer+n;
	buf->sz = so.sz - n;
	buf->buffer = request->buffer;
	buf->next = NULL;
	if (s->head == NULL) {
		s->head = s->tail = buf;
	} else {
		assert(s->tail != NULL);
		assert(s->tail->next == NULL);
		s->tail->next = buf;
		s->tail = buf;
	}
	return buf;
}

/* 将一个 UDP 发送数据添加到相应的队列中去, 相应的对端地址会被添加到写缓冲中去, 并增加写缓冲的大小.
 *
 * 参数: ss 是套接字服务器; s 是写缓冲队列对应的套接字; priority 是高低权限队列的标记, 有 PRIORITY_HIGH 和 PRIORITY_LOW 两个选项;
 * request 是包含了发送数据的请求体; udp_address 是对端的地址
 *
 * 返回: 函数没有返回值
 * */
static inline void
append_sendbuffer_udp(struct socket_server *ss, struct socket *s, int priority, struct request_send * request, const uint8_t udp_address[UDP_ADDRESS_SIZE]) {
	struct wb_list *wl = (priority == PRIORITY_HIGH) ? &s->high : &s->low;
	struct write_buffer *buf = append_sendbuffer_(ss, wl, request, SIZEOF_UDPBUFFER, 0);
	memcpy(buf->udp_address, udp_address, UDP_ADDRESS_SIZE);
	s->wb_size += buf->sz;
}

/* 将第一个 TCP 发送数据添加到相应的高权限队列中去, 所添加的数据是从 request 中 buf 的第 n 个字节开始的, 最终会增加写缓冲的大小.
 * 参数: ss 是套接字服务器; s 是写缓冲队列对应的套接字; request 是发送数据的请求体; n 是待发送数据的起始字节
 * 返回: 函数无返回值 */
static inline void
append_sendbuffer(struct socket_server *ss, struct socket *s, struct request_send * request, int n) {
	struct write_buffer *buf = append_sendbuffer_(ss, &s->high, request, SIZEOF_TCPBUFFER, n);
	s->wb_size += buf->sz;
}

/* 将第一个 TCP 发送数据添加到相应的低权限队列中去, 数据来自于 request 请求体中, 最终会增加写缓冲的大小.
 * 参数: ss 是套接字服务器; s 是写缓冲队列对应的套接字; request 是发送数据的请求体
 * 返回: 函数无返回值 */
static inline void
append_sendbuffer_low(struct socket_server *ss,struct socket *s, struct request_send * request) {
	struct write_buffer *buf = append_sendbuffer_(ss, &s->low, request, SIZEOF_TCPBUFFER, 0);
	s->wb_size += buf->sz;
}

/* 检查套接字的写缓冲是否是空的, 写缓冲包括高权限和低权限两条队列. */
static inline int
send_buffer_empty(struct socket *s) {
	return (s->high.head == NULL && s->low.head == NULL);
}

/*
	When send a package , we can assign the priority : PRIORITY_HIGH or PRIORITY_LOW

	If socket buffer is empty, write to fd directly.
		If write a part, append the rest part to high list. (Even priority is PRIORITY_LOW)
	Else append package to high (PRIORITY_HIGH) or low (PRIORITY_LOW) list.
 */

/* 向套接字中发送数据, 此函数可以处理依据套接字的协议类型同时处理 TCP 和 UDP 的数据包. 如果套接字不处于合适的状态下, 将不发送数据.
 * 当写缓冲队列为空且套接字处于连接状态, 将直接写入数据, 如果只写入了一部分, 剩下的将被添加到高权限写缓冲队列中去. 状态为连接中或者绑定的
 * 套接字将依据权限高低放到相应的写缓冲队列中.
 *
 * 参数: ss 是套接字服务器; request 中包含了套接字 id 和发送数据; 出参 result 仅用于出错时接收关闭套接字的结果;
 * priority 是写缓冲队列的权限标记; udp_address 仅在 UDP 协议的情况下提供对端地址
 *
 * 返回: 发送成功时将返回 -1 , 如果发送失败将关闭套接字并返回 SOCKET_CLOSE */
static int
send_socket(struct socket_server *ss, struct request_send * request, struct socket_message *result, int priority, const uint8_t *udp_address) {
	int id = request->id;
	struct socket * s = &ss->slot[HASH_ID(id)];

	/* 仅当构建好的发送对象处于写缓冲队列外部时, 才会调用对象本身的 free_func, 在队列中将调用套接字服务器的释放方法 */
	struct send_object so;
	send_object_init(ss, &so, request->buffer, request->sz);
	
	/* [ck]SOCKET_TYPE_RESERVE 也不应该发送数据, 或者添加到写缓冲队列中去,
	 * 它还没有关联文件描述符不能发送数据[/ck] */
	if (s->type == SOCKET_TYPE_INVALID || s->id != id 
		|| s->type == SOCKET_TYPE_HALFCLOSE
		|| s->type == SOCKET_TYPE_PACCEPT) {
		so.free_func(request->buffer);
		return -1;
	}

	/* 用于侦听端口的套接字是不会发送数据的 */
	if (s->type == SOCKET_TYPE_PLISTEN || s->type == SOCKET_TYPE_LISTEN) {
		fprintf(stderr, "socket-server: write to listen fd %d.\n", id);
		so.free_func(request->buffer);
		return -1;
	}
	if (send_buffer_empty(s) && s->type == SOCKET_TYPE_CONNECTED) {
		if (s->protocol == PROTOCOL_TCP) {
			int n = write(s->fd, so.buffer, so.sz);
			if (n<0) {
				switch(errno) {
				case EINTR:
				case AGAIN_WOULDBLOCK:
					n = 0;
					break;
				default:
					fprintf(stderr, "socket-server: write to %d (fd=%d) error :%s.\n",id,s->fd,strerror(errno));
					force_close(ss,s,result);
					so.free_func(request->buffer);
					return SOCKET_CLOSE;
				}
			}
			/* 如果数据都全部发送完了, 将直接返回, 否则将剩余的未发送的数据添加到高权限写缓冲队列中 */
			if (n == so.sz) {
				so.free_func(request->buffer);
				return -1;
			}
			append_sendbuffer(ss, s, request, n);	// add to high priority list, even priority == PRIORITY_LOW
		} else {
			// udp
			if (udp_address == NULL) {
				/* [ck]需要检查 udp_address 是否是正确的值, 在最初生成套接字时, 这个是一个全部为 0 的数组[/ck] */
				udp_address = s->p.udp_address;
			}
			union sockaddr_all sa;
			socklen_t sasz = udp_socket_address(s, udp_address, &sa);
			int n = sendto(s->fd, so.buffer, so.sz, 0, &sa.s, sasz);
			/* 对于 UDP 如果没有发送完整的包或者发送失败, 将放到相应的队列中并在以后重新发送 */
			if (n != so.sz) {
				append_sendbuffer_udp(ss,s,priority,request,udp_address);
			} else {
				so.free_func(request->buffer);
				return -1;
			}
		}

		/* 执行到此处表示有一部分数据被放到了队列中去了, 因而需要添加一个可写事件侦听 */
		sp_write(ss->event_fd, s->fd, s, true);
	} else {
		if (s->protocol == PROTOCOL_TCP) {
			if (priority == PRIORITY_LOW) {
				append_sendbuffer_low(ss, s, request);
			} else {
				append_sendbuffer(ss, s, request, 0);
			}
		} else {
			if (udp_address == NULL) {
				udp_address = s->p.udp_address;
			}
			append_sendbuffer_udp(ss,s,priority,request,udp_address);
		}
	}
	return -1;
}

/* 将已经处于 LISTEN 状态的套接字文件描述符与 skynet 的套接字关联. 如果失败将由出参 result 提示失败的原因.
 * 如果成功, 套接字的状态类型将变为 SOCKET_TYPE_PLISTEN , 失败时将变为 SOCKET_TYPE_INVALID 并且文件描述将被关闭.
 *
 * 参数: ss 是套接字服务器; request 中包含了文件描述、套接字 id 和服务句柄; 出参 result 提示返回失败的原因;
 * 返回: 成功时返回 -1 , 失败是返回 SOCKET_ERROR . */
static int
listen_socket(struct socket_server *ss, struct request_listen * request, struct socket_message *result) {
	int id = request->id;
	int listen_fd = request->fd;
	struct socket *s = new_fd(ss, id, listen_fd, PROTOCOL_TCP, request->opaque, false);
	if (s == NULL) {
		goto _failed;
	}
	s->type = SOCKET_TYPE_PLISTEN;
	return -1;
_failed:
	close(listen_fd);
	result->opaque = request->opaque;
	result->id = id;
	result->ud = 0;
	/* 在 SOCKET_ERROR 情况下返回的 data 必须不是需要回收的堆内存, 因为其内存最终不会被释放,
	 * 而是复制其内容 */
	result->data = "reach skynet socket number limit";
	ss->slot[HASH_ID(id)].type = SOCKET_TYPE_INVALID;

	return SOCKET_ERROR;
}

/* 关闭套接字. 如果套接字原先已经被关闭, 将不做任何事情返回 SOCKET_CLOSE . 否则将尽可能去发送套接字中的缓冲数据.
 * 并且在缓冲为空或者要求立即关闭的情况下, 关闭套接字. 不为空的情况下套接字将进入 SOCKET_TYPE_HALFCLOSE 状态,
 * 返回 -1 .
 *
 * 参数: ss 是套接字服务器; request 是请求体包含了需要关闭的套接字 id、服务句柄和立即关闭的标记;
 * 出参 result 仅在套接字完全关闭的情况下包含数据;
 *
 * 返回: 完全关闭的情况下返回 SOCKET_CLOSE , 半关闭时返回 -1 . */
static int
close_socket(struct socket_server *ss, struct request_close *request, struct socket_message *result) {
	int id = request->id;
	struct socket * s = &ss->slot[HASH_ID(id)];
	/* 如果套接字已经关闭, 或者槽被分配给了别的套接字, 将直接返回关闭 */
	if (s->type == SOCKET_TYPE_INVALID || s->id != id) {
		result->id = id;
		result->opaque = request->opaque;
		result->ud = 0;
		result->data = NULL;
		return SOCKET_CLOSE;
	}
	if (!send_buffer_empty(s)) { 
		int type = send_buffer(ss,s,result);
		if (type != -1)
			return type;
	}
	/* 当要求立即关闭或者发送缓冲已经为空的情况下, 将关闭套接字并返回关闭 */
	if (request->shutdown || send_buffer_empty(s)) {
		force_close(ss,s,result);
		result->id = id;
		result->opaque = request->opaque;
		return SOCKET_CLOSE;
	}
	/* 其它情况返回半关闭的状态 */
	s->type = SOCKET_TYPE_HALFCLOSE;

	return -1;
}

/* 将一个操作系统别处的套接字文件描述符关联到 skynet 的套接字中. 因为是操作系统中别处的文件描述符, 将不由 skynet 关闭.
 * 关联成功后套接字的状态将为 SOCKET_TYPE_BIND , 并且文件描述符将被设置为非阻塞的.
 *
 * 参数: ss 是套接字服务器; request 是绑定请求的请求体; 出参 result 返回套接字信息;
 * 返回: 无法绑定时返回 SOCKET_ERROR , 绑定成功时返回 SOCKET_OPEN . */
static int
bind_socket(struct socket_server *ss, struct request_bind *request, struct socket_message *result) {
	int id = request->id;
	result->id = id;
	result->opaque = request->opaque;
	result->ud = 0;
	struct socket *s = new_fd(ss, id, request->fd, PROTOCOL_TCP, request->opaque, true);
	if (s == NULL) {
		result->data = "reach skynet socket number limit";
		return SOCKET_ERROR;
	}
	sp_nonblocking(request->fd);
	s->type = SOCKET_TYPE_BIND;
	result->data = "binding";
	return SOCKET_OPEN;
}

/* 将处于 SOCKET_TYPE_PACCEPT 和 SOCKET_TYPE_PLISTEN 状态的套接字继续向前推进, 使得可以接收 I/O 事件通知.
 * 如果套接字处于 SOCKET_TYPE_CONNECTED 状态, 将被转移到别的服务中去.
 *
 * 参数: ss 是套接字服务器; request 是开启套接字的请求体; result 为出参;
 * 返回: 当发生错误时返回 SOCKET_ERROR , 如果是转移或者推进状态将返回 SOCKET_OPEN, 其它情况下返回 -1 . */
static int
start_socket(struct socket_server *ss, struct request_start *request, struct socket_message *result) {
	int id = request->id;
	result->id = id;
	result->opaque = request->opaque;
	result->ud = 0;
	result->data = NULL;
	struct socket *s = &ss->slot[HASH_ID(id)];
	/* 状态不匹配, 或者查询到的套接字不是期望的套接字将不执行并返回错误 */
	if (s->type == SOCKET_TYPE_INVALID || s->id != id) {
		result->data = "invalid socket";
		return SOCKET_ERROR;
	}
	/* 将两种状态下的套接字推进并接收 I/O 事件, 失败将导致套接字关闭;
	 * 需要注意的是状态推进和转移将导致套接字的 opaque 发生变化. */
	if (s->type == SOCKET_TYPE_PACCEPT || s->type == SOCKET_TYPE_PLISTEN) {
		if (sp_add(ss->event_fd, s->fd, s)) {
			force_close(ss, s, result);
			result->data = strerror(errno);
			return SOCKET_ERROR;
		}
		s->type = (s->type == SOCKET_TYPE_PACCEPT) ? SOCKET_TYPE_CONNECTED : SOCKET_TYPE_LISTEN;
		s->opaque = request->opaque;
		result->data = "start";
		return SOCKET_OPEN;
	} else if (s->type == SOCKET_TYPE_CONNECTED) {
		// todo: maybe we should send a message SOCKET_TRANSFER to s->opaque
		s->opaque = request->opaque;
		result->data = "transfer";
		return SOCKET_OPEN;
	}
	// if s->type == SOCKET_TYPE_HALFCLOSE , SOCKET_CLOSE message will send later
	return -1;
}

/* 设置套接字选项, 选项的层次在 IPPROTO_TCP 上 , 设置的键和值都是 int 类型的, 目前仅用于设置套接字的 TCP_NODELAY 选项
 * 参数: ss 是套接字服务器; request 是设置套接字选项的请求体;
 * 函数无返回值 */
static void
setopt_socket(struct socket_server *ss, struct request_setopt *request) {
	int id = request->id;
	struct socket *s = &ss->slot[HASH_ID(id)];
	if (s->type == SOCKET_TYPE_INVALID || s->id !=id) {
		return;
	}
	int v = request->value;
	setsockopt(s->fd, IPPROTO_TCP, request->what, &v, sizeof(v));
}

/* 从管道中读取数据并保存在缓冲中, 管道中的数据大小一定是 sz , 并且保证小于 256 个字节, 这是由 skynet 系统保证的.
 * 参数: pipefd 是管道的文件描述符; buffer 是接收数据的缓冲; sz 是数据的大小, 保证管道中一定是等量的.
 * 返回: 函数无返回值 */
static void
block_readpipe(int pipefd, void *buffer, int sz) {
	for (;;) {
		int n = read(pipefd, buffer, sz);
		if (n<0) {
			/* 如果被中断则再次执行, 否则意味着出现了别的错误 */
			if (errno == EINTR)
				continue;
			fprintf(stderr, "socket-server : read pipe error %s.\n",strerror(errno));
			return;
		}
		// must atomic read from a pipe
		assert(n == sz);
		return;
	}
}

/* 检查是否有命令, 方式是检查接收命令的管道文件描述符是否有可读事件. 此函数是非阻塞的. 不论是否有数据都将立即返回.
 * 虽然管道的两个文件描述符是阻塞的, 但是此检查函数却是非阻塞的.
 * 参数: ss 是套接字服务器;
 * 返回: 1 表示有命令, 0 表示没有命令. */
static int
has_cmd(struct socket_server *ss) {
	struct timeval tv = {0,0};
	int retval;

	FD_SET(ss->recvctrl_fd, &ss->rfds);

	retval = select(ss->recvctrl_fd+1, &ss->rfds, NULL, NULL, &tv);
	if (retval == 1) {
		return 1;
	}
	return 0;
}

/* 增加一个 UDP 的套接字, 参数 udp 中包含预先分配好的系统 UDP 套接字文件描述符和套接字插槽.
 * 添加成功之后的 UDP 套接字是没有关联对端地址的, 函数支持 AF_INET 和 AF_INET6 两种协议,
 * 在添加失败时将导致操作系统套接字关闭, 并且 skynet 中的套接字插槽归还.
 *
 * 参数: ss 是套接字服务器; udp 是 UDP 套接字生成的请求参数;
 * 函数没有返回值. */
static void
add_udp_socket(struct socket_server *ss, struct request_udp *udp) {
	int id = udp->id;
	int protocol;
	if (udp->family == AF_INET6) {
		protocol = PROTOCOL_UDPv6;
	} else {
		protocol = PROTOCOL_UDP;
	}
	struct socket *ns = new_fd(ss, id, udp->fd, protocol, udp->opaque, true);
	if (ns == NULL) {
		close(udp->fd);
		ss->slot[HASH_ID(id)].type = SOCKET_TYPE_INVALID;
		return;
	}
	ns->type = SOCKET_TYPE_CONNECTED;
	memset(ns->p.udp_address, 0, sizeof(ns->p.udp_address));
}

/* 给 UDP 类型的套接字设置对端地址. 如果地址中包含的类型和套接字的类型不匹配将无法完成设置.
 * 参数: ss 是套接字服务器; request 是设置 UDP 地址的请求体; 出参 result 仅在设置失败时包含内容;
 * 返回: 成功时返回 -1 , 错误时返回 SOCKET_ERROR . */
static int
set_udp_address(struct socket_server *ss, struct request_setudp *request, struct socket_message *result) {
	int id = request->id;
	struct socket *s = &ss->slot[HASH_ID(id)];
	if (s->type == SOCKET_TYPE_INVALID || s->id !=id) {
		return -1;
	}
	int type = request->address[0];
	if (type != s->protocol) {
		// protocol mismatch
		result->opaque = s->opaque;
		result->id = s->id;
		result->ud = 0;
		result->data = "protocol mismatch";

		return SOCKET_ERROR;
	}
	if (type == PROTOCOL_UDP) {
		memcpy(s->p.udp_address, request->address, 1+2+4);	// 1 type, 2 port, 4 ipv4
	} else {
		memcpy(s->p.udp_address, request->address, 1+2+16);	// 1 type, 2 port, 16 ipv6
	}
	return -1;
}

// return type
/* 从管道中读取命令并执行相应的命令, 把执行的结果放入出参 result 中返回, 调用者可以依据函数的返回值和 result 来决定下一步的动作.
 * 结果为 -1 表示套接字状态未发生改变, 通常调用者可以继续下一次的调用, 如 socket_server_poll 函数所为. 如果套接字的状态
 * 发生了改变, 如关闭(SOCKET_CLOSE)、出错(SOCKET_ERROR)、打开(SOCKET_OPEN)、服务器退出(SOCKET_EXIT) 将返回给调用者.
 *
 * 参数: ss 是套接字服务器; 出参 result 用于接收命令处理的结果;
 * 返回: -1 表示状态不发生改变, 其它值在 socket_server.h 中定义的 7 中状态中的一种, 表示状态发生了改变. */
static int
ctrl_cmd(struct socket_server *ss, struct socket_message *result) {
	int fd = ss->recvctrl_fd;
	// the length of message is one byte, so 256+8 buffer size is enough.
	uint8_t buffer[256];
	uint8_t header[2];
	block_readpipe(fd, header, sizeof(header));
	int type = header[0];
	int len = header[1];
	block_readpipe(fd, buffer, len);
	// ctrl command only exist in local fd, so don't worry about endian.
	switch (type) {
	case 'S':
		return start_socket(ss,(struct request_start *)buffer, result);
	case 'B':
		return bind_socket(ss,(struct request_bind *)buffer, result);
	case 'L':
		return listen_socket(ss,(struct request_listen *)buffer, result);
	case 'K':
		return close_socket(ss,(struct request_close *)buffer, result);
	case 'O':
		return open_socket(ss, (struct request_open *)buffer, result);
	/* 套接字服务器退出时其状态并没有发生改变, 而是由上层决定结束其处理线程和销毁服务器的内存 */
	case 'X':
		result->opaque = 0;
		result->id = 0;
		result->ud = 0;
		result->data = NULL;
		return SOCKET_EXIT;
	case 'D':
		return send_socket(ss, (struct request_send *)buffer, result, PRIORITY_HIGH, NULL);
	case 'P':
		return send_socket(ss, (struct request_send *)buffer, result, PRIORITY_LOW, NULL);
	case 'A': {
		struct request_send_udp * rsu = (struct request_send_udp *)buffer;
		return send_socket(ss, &rsu->send, result, PRIORITY_HIGH, rsu->address);
	}
	case 'C':
		return set_udp_address(ss, (struct request_setudp *)buffer, result);
	case 'T':
		setopt_socket(ss, (struct request_setopt *)buffer);
		return -1;
	case 'U':
		add_udp_socket(ss, (struct request_udp *)buffer);
		return -1;
	default:
		fprintf(stderr, "socket-server: Unknown ctrl %c.\n",type);
		return -1;
	};

	return -1;
}

// return -1 (ignore) when error
/* 从 TCP 类型的套接字中读取数据并将数据放入到 result 中, 如果成功将返回 SOCKET_DATA 并携带数据.
 * 在出错或关闭的情况下, result 将携带关闭信息. 其它情况下返回 -1 并且 result 不携带任何信息.
 *
 * 参数: ss 是套接字服务器; s 是需要读取数据的套接字; 出参 result 用于接收数据读取结果;
 * 返回: SOCKET_DATA 表明有数据, SOCKET_ERROR 表示读取出错, SOCKET_CLOSE 表示套接字已经关闭, -1 表示状态不改变. */
static int
forward_message_tcp(struct socket_server *ss, struct socket *s, struct socket_message * result) {
	int sz = s->p.size;
	char * buffer = MALLOC(sz);
	int n = (int)read(s->fd, buffer, sz);
	if (n<0) {
		FREE(buffer);
		switch(errno) {
		case EINTR:
			break;
		case AGAIN_WOULDBLOCK:
			fprintf(stderr, "socket-server: EAGAIN capture.\n");
			break;
		default:
			// close when error
			force_close(ss, s, result);
			result->data = strerror(errno);
			return SOCKET_ERROR;
		}
		return -1;
	}
	/* [ck]在可读的情况下读取的数据量为 0 , 表明对端关闭了套接字[/ck] */
	if (n==0) {
		FREE(buffer);
		force_close(ss, s, result);
		return SOCKET_CLOSE;
	}

	if (s->type == SOCKET_TYPE_HALFCLOSE) {
		// discard recv data
		FREE(buffer);
		return -1;
	}

	if (n == sz) {
		s->p.size *= 2;
	} else if (sz > MIN_READ_BUFFER && n*2 < sz) {
		s->p.size /= 2;
	}

	result->opaque = s->opaque;
	result->id = s->id;
	result->ud = n;
	result->data = buffer;
	return SOCKET_DATA;
}

/* 依据 sa 中的套接字地址, 填充到 udp_address 中, 填充的方式是头字节是协议类型, 可能为 UDP 或者 UDPv6 两种,
 * 接下来是两个字节是端口号, 剩下的为 ip , ip 可能是 ipv4(32bit) 也可能是 ipv6(128bit) .
 *
 * 参数: protocol 是协议类型, 分为 UDP 和 UDPv6 两种; sa 是系统的套接字地址; udp_address 作为出参接收最终形式的地址;
 * 返回: 地址的字节数 */
static int
gen_udp_address(int protocol, union sockaddr_all *sa, uint8_t * udp_address) {
	int addrsz = 1;
	udp_address[0] = (uint8_t)protocol;
	if (protocol == PROTOCOL_UDP) {
		memcpy(udp_address+addrsz, &sa->v4.sin_port, sizeof(sa->v4.sin_port));
		addrsz += sizeof(sa->v4.sin_port);
		memcpy(udp_address+addrsz, &sa->v4.sin_addr, sizeof(sa->v4.sin_addr));
		addrsz += sizeof(sa->v4.sin_addr);
	} else {
		memcpy(udp_address+addrsz, &sa->v6.sin6_port, sizeof(sa->v6.sin6_port));
		addrsz += sizeof(sa->v6.sin6_port);
		memcpy(udp_address+addrsz, &sa->v6.sin6_addr, sizeof(sa->v6.sin6_addr));
		addrsz += sizeof(sa->v6.sin6_addr);
	}
	return addrsz;
}

/* 从 UDP 或者 UDPv6 类型的套接字中读取数据, 得到的数据将通过 result 回传给调用者, 最终成功将返回 SOCKET_UDP .
 * 如果发生了错误将导致套接字被关闭, 并且返回 SOCKET_ERROR , result 中包含关闭信息. 其它情况下将返回 -1 并且不读取任何数据.
 *
 * 参数: ss 是套接字服务器; s 是需要读取数据的套接字; 出参 result 用于接收数据, 也有可能是关闭的信息;
 * 返回: SOCKET_UDP 表示读取成功; SOCKET_ERROR 表示读取出错; -1 表示状态不发生改变; */
static int
forward_message_udp(struct socket_server *ss, struct socket *s, struct socket_message * result) {
	union sockaddr_all sa;
	socklen_t slen = sizeof(sa);
	int n = recvfrom(s->fd, ss->udpbuffer,MAX_UDP_PACKAGE,0,&sa.s,&slen);
	if (n<0) {
		switch(errno) {
		case EINTR:
		case AGAIN_WOULDBLOCK:
			break;
		default:
			// close when error
			force_close(ss, s, result);
			result->data = strerror(errno);
			return SOCKET_ERROR;
		}
		return -1;
	}
	uint8_t * data;
	if (slen == sizeof(sa.v4)) {
		if (s->protocol != PROTOCOL_UDP)
			return -1;
		data = MALLOC(n + 1 + 2 + 4);
		gen_udp_address(PROTOCOL_UDP, &sa, data + n);
	} else {
		if (s->protocol != PROTOCOL_UDPv6)
			return -1;
		data = MALLOC(n + 1 + 2 + 16);
		gen_udp_address(PROTOCOL_UDPv6, &sa, data + n);
	}
	memcpy(data, ss->udpbuffer, n);

	result->opaque = s->opaque;
	result->id = s->id;
	result->ud = n;
	result->data = (char *)data;

	return SOCKET_UDP;
}

/* 报告连接是否完成, 当套接字连接时不能马上完成时, 可以通过 poll 函数来检测套接字的可写状态. 如果可写表示连接完成.
 * 之后调用 getsocketopt 读取 SOL_SOCKET 的 SO_ERROR 选项来判断是否成功, 成功时 SO_ERROR 将为 0 .
 *
 * 参数: ss 是套接字服务器; s 是未完成连接的套接字; 出参 result 用于接收连接成功的信息或者失败关闭的信息;
 * 返回: 成功时返回 SOCKET_OPEN , 失败时将导致套接字关闭并返回 SOCKET_ERROR . */
static int
report_connect(struct socket_server *ss, struct socket *s, struct socket_message *result) {
	int error;
	socklen_t len = sizeof(error);  
	int code = getsockopt(s->fd, SOL_SOCKET, SO_ERROR, &error, &len);
	if (code < 0 || error) {
		force_close(ss,s, result);
		/* code 有可能大于 0 的原因在于自定义的套接字选项的处理器会返回正值 */
		if (code >= 0)
			result->data = strerror(error);
		else
			result->data = strerror(errno);
		return SOCKET_ERROR;
	} else {
		s->type = SOCKET_TYPE_CONNECTED;
		result->opaque = s->opaque;
		result->id = s->id;
		result->ud = 0;
		if (send_buffer_empty(s)) {
			sp_write(ss->event_fd, s->fd, s, false);
		}
		union sockaddr_all u;
		socklen_t slen = sizeof(u);
		if (getpeername(s->fd, &u.s, &slen) == 0) {
			void * sin_addr = (u.s.sa_family == AF_INET) ? (void*)&u.v4.sin_addr : (void *)&u.v6.sin6_addr;
			if (inet_ntop(u.s.sa_family, sin_addr, ss->buffer, sizeof(ss->buffer))) {
				result->data = ss->buffer;
				return SOCKET_OPEN;
			}
		}
		result->data = NULL;
		return SOCKET_OPEN;
	}
}

// return 0 when failed, or -1 when file limit
/* 检测侦听端口的套接字中是否有连接请求, 如果有则接受请求并生成一个新的套接字. 新生成的套接字由出参 result 接收并返回.
 * 当由于打开过多文件描述符而导致无法接受请求时将返回 -1 . 或者由于别的原因导致不能接受请求时返回 0 .
 *
 * 参数: ss 是套接字服务器; s 是准备接受连接的套接字; 出参 result 用于接收成功后的结果, 其 ud 字段是新的套接字 id,
 * 如果失败将用于接收失败的信息;
 *
 * 返回: 0 表示未能正确接收连接, -1 表示打开的文件超出限制, 1 表示成功 */
static int
report_accept(struct socket_server *ss, struct socket *s, struct socket_message *result) {
	union sockaddr_all u;
	socklen_t len = sizeof(u);
	int client_fd = accept(s->fd, &u.s, &len);
	if (client_fd < 0) {
		if (errno == EMFILE || errno == ENFILE) {
			result->opaque = s->opaque;
			result->id = s->id;
			result->ud = 0;
			result->data = strerror(errno);
			return -1;
		} else {
			return 0;
		}
	}
	int id = reserve_id(ss);
	if (id < 0) {
		close(client_fd);
		return 0;
	}
	socket_keepalive(client_fd);
	sp_nonblocking(client_fd);
	struct socket *ns = new_fd(ss, id, client_fd, PROTOCOL_TCP, s->opaque, false);
	if (ns == NULL) {
		close(client_fd);
		return 0;
	}
	ns->type = SOCKET_TYPE_PACCEPT;
	result->opaque = s->opaque;
	result->id = s->id;
	result->ud = id;
	result->data = NULL;

	void * sin_addr = (u.s.sa_family == AF_INET) ? (void*)&u.v4.sin_addr : (void *)&u.v6.sin6_addr;
	int sin_port = ntohs((u.s.sa_family == AF_INET) ? u.v4.sin_port : u.v6.sin6_port);
	char tmp[INET6_ADDRSTRLEN];
	if (inet_ntop(u.s.sa_family, sin_addr, tmp, sizeof(tmp))) {
		snprintf(ss->buffer, sizeof(ss->buffer), "%s:%d", tmp, sin_port);
		result->data = ss->buffer;
	}

	return 1;
}

/* 当套接字发生错误或者关闭时, 将剩余的未处理的事件关闭. 其中 result 包含套接字, result 是上一个套接字处理的结果.
 * 参数: ss 是套接字服务器; result 是套接字处理的结果, 里边包含了一个套接字 id; type 是结果类型, 只有 SOCKET_CLOSE 和 SOCKET_ERROR 会被处理.
 * 函数没有返回值 */
static inline void 
clear_closed_event(struct socket_server *ss, struct socket_message * result, int type) {
	if (type == SOCKET_CLOSE || type == SOCKET_ERROR) {
		int id = result->id;
		int i;
		for (i=ss->event_index; i<ss->event_n; i++) {
			struct event *e = &ss->ev[i];
			struct socket *s = e->s;
			if (s) {
				if (s->type == SOCKET_TYPE_INVALID && s->id == id) {
					e->s = NULL;
					break;
				}
			}
		}
	}
}

// return type
/* 套接字服务器的处理函数, 此函数将依次检查并执行发过来的套接字指令, 当 I/O 事件处理完之后阻塞等待 I/O 事件
 * 或者如果没有处理完将处理最近的套接字事件, 事件的优先顺序是先处理未完成的连接和侦听接收事件, 其后才是对套接字进行读写.
 *
 * 参数: ss 是套接字服务器; 出参 result 用于接收各种处理的结果; 出参 more 表示上次的事件列表还未处理完, 0 表示已经处理完成;
 * 返回: socket_server.h 中定义的 socket 的事件类型, 或者 -1 表示等待 I/O 事件失败. */
int 
socket_server_poll(struct socket_server *ss, struct socket_message * result, int * more) {
	for (;;) {
		/* 虽然是优先处理套接字命令, 但处理过一次之后, 需要先等待处理完上次的套接字事件才会接着处理套接字命令 */
		if (ss->checkctrl) {
			if (has_cmd(ss)) {
				/* 处理套接字命令, -1 将接着再处理一次 */
				int type = ctrl_cmd(ss, result);
				if (type != -1) {
					clear_closed_event(ss, result, type);
					return type;
				} else {
					continue;
				}
			} else {
				ss->checkctrl = 0;
			}
		}
		if (ss->event_index == ss->event_n) {
			ss->event_n = sp_wait(ss->event_fd, ss->ev, MAX_EVENT);
			ss->checkctrl = 1;
			if (more) {
				*more = 0;
			}
			ss->event_index = 0;
			if (ss->event_n <= 0) {
				ss->event_n = 0;
				return -1;
			}
		}
		struct event *e = &ss->ev[ss->event_index++];
		struct socket *s = e->s;
		if (s == NULL) {
			// dispatch pipe message at beginning
			continue;
		}
		switch (s->type) {
		case SOCKET_TYPE_CONNECTING:
			return report_connect(ss, s, result);
		case SOCKET_TYPE_LISTEN: {
			int ok = report_accept(ss, s, result);
			if (ok > 0) {
				return SOCKET_ACCEPT;
			} if (ok < 0 ) {
				return SOCKET_ERROR;
			}
			// when ok == 0, retry
			break;
		}
		case SOCKET_TYPE_INVALID:
			fprintf(stderr, "socket-server: invalid socket\n");
			break;
		default:
			if (e->read) {
				int type;
				if (s->protocol == PROTOCOL_TCP) {
					type = forward_message_tcp(ss, s, result);
				} else {
					type = forward_message_udp(ss, s, result);
					if (type == SOCKET_UDP) {
						// try read again
						/* [ck]为何需要再读一次?[/ck] */
						--ss->event_index;
						return SOCKET_UDP;
					}
				}
				if (e->write && type != SOCKET_CLOSE && type != SOCKET_ERROR) {
					// Try to dispatch write message next step if write flag set.
					e->read = false;
					--ss->event_index;
				}
				/* -1 表示读取当状态不变, 将不再处理此套接字的读取事件, 而是继续处理其它事件 */
				if (type == -1)
					break;				
				return type;
			}
			if (e->write) {
				int type = send_buffer(ss, s, result);
				if (type == -1)
					break;
				return type;
			}
			break;
		}
	}
}

/* 将套接字的命令写入到管道中去, 通过 recvctrl_fd 文件描述符可以从管道中读取数据并执行相应的命令.
 *
 * 参数: ss 是套接字服务器; request 是请求体, 其第 9 个字节开始为真正的请求体内容, 长度为 len ;
 *      type 是请求的类型, 用于后面的分发动作; len 是请求体内容的长度;
 *
 * 函数无返回值
 */
static void
send_request(struct socket_server *ss, struct request_package *request, char type, int len) {
	/* 7、8 个字节用于写入命令的类型和长度 */
	request->header[6] = (uint8_t)type;
	request->header[7] = (uint8_t)len;
	for (;;) {
		int n = write(ss->sendctrl_fd, &request->header[6], len+2);
		if (n<0) {
			if (errno != EINTR) {
				fprintf(stderr, "socket-server : send ctrl command error %s.\n", strerror(errno));
			}
			continue;
		}
		assert(n == len+2);
		return;
	}
}

/* 生成一个 TCP 连接请求对象, 要求地址 addr 的长度不能超过 256 个字节.
 * 参数: ss 是套接字服务器; 出参 req 将包含 TCP 连接请求体; opaque 是服务句柄; addr 是对象主机名; port 是对端服务端口;
 * 返回: 主机名的长度, 如果 addr 过长或者无法获取到套接字插槽将失败并返回 -1 . */
static int
open_request(struct socket_server *ss, struct request_package *req, uintptr_t opaque, const char *addr, int port) {
	int len = strlen(addr);
	if (len + sizeof(req->u.open) >= 256) {
		fprintf(stderr, "socket-server : Invalid addr %s.\n",addr);
		return -1;
	}
	int id = reserve_id(ss);
	if (id < 0)
		return -1;
	req->u.open.opaque = opaque;
	req->u.open.id = id;
	req->u.open.port = port;
	memcpy(req->u.open.host, addr, len);
	req->u.open.host[len] = '\0';

	return len;
}

/* 发起一个 TCP 连接, 并返回最终成功的套接字标识符.
 * 参数: ss 是套接字服务器; opaque 是服务句柄; addr 是对端地址; port 是对端服务端口号;
 * 返回: 成功时返回套接字 id, 失败时返回 -1 . */
int 
socket_server_connect(struct socket_server *ss, uintptr_t opaque, const char * addr, int port) {
	struct request_package request;
	int len = open_request(ss, &request, opaque, addr, port);
	if (len < 0)
		return -1;
	send_request(ss, &request, 'O', sizeof(request.u.open) + len);
	return request.u.open.id;
}

/* 释放缓存的内存, 释放的方法是先生成一个 send_object , 其中包含了释放缓存的函数.
 * 之所以这样的原因是此模块支持两种生成缓存的方式.
 *
 * 参数: ss 是套接字服务器; buffer 是缓存; sz 是缓存的大小;
 * 函数无返回值 */
static void
free_buffer(struct socket_server *ss, const void * buffer, int sz) {
	struct send_object so;
	send_object_init(ss, &so, (void *)buffer, sz);
	so.free_func((void *)buffer);
}

// return -1 when error
/* 向套接字发送高权限的数据, 此函数可以用于 TCP 和 UDP (需要先调用 socket_server_udp_connect ) 两种协议的套接字.
 * 发送的过程是异步的, 函数先将内容写入到管道中, 并由专门的处理线程完成发送操作, 或者写入到低权限的写缓冲队列中去.
 *
 * 参数: ss 是套接字服务器; id 是发送数据的套接字标识; buffer 是发送的数据内容; sz 是发送的大小;
 * 返回: 套接字中的缓冲数据大小, 如果失败将返回 -1 . */
int64_t 
socket_server_send(struct socket_server *ss, int id, const void * buffer, int sz) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		free_buffer(ss, buffer, sz);
		return -1;
	}

	struct request_package request;
	request.u.send.id = id;
	request.u.send.sz = sz;
	request.u.send.buffer = (char *)buffer;

	send_request(ss, &request, 'D', sizeof(request.u.send));
	return s->wb_size;
}

/* 向套接字发送低权限的数据, 此函数可以用于 TCP 和 UDP(需要先调用 socket_server_udp_connect ) 两种协议的套接字.
 * 发送的过程是异步的, 函数先将内容写入到管道中, 并由专门的处理线程完成发送操作, 或者写入到低权限的写缓冲队列中去.
 *
 * 参数: ss 是套接字服务器; id 是套接字标识; buffer 是发送的内容; sz 是发送的内容的大小;
 * 函数无返回值 */
void 
socket_server_send_lowpriority(struct socket_server *ss, int id, const void * buffer, int sz) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		free_buffer(ss, buffer, sz);
		return;
	}

	struct request_package request;
	request.u.send.id = id;
	request.u.send.sz = sz;
	request.u.send.buffer = (char *)buffer;

	send_request(ss, &request, 'P', sizeof(request.u.send));
}

/* 退出整个套接字服务器命令, 调用此函数并不是真正销毁套接字服务器而是以异步的方式给处理线程返回一个 SOCKET_EXIT 状态.
 * 这样处理线程可以安全的退出, 从而不再处理套接字事件. 真正销毁内存实际上是在整个 skynet 系统退出时.
 *
 * 参数: ss 是套接字服务器
 * 函数无返回值, 会异步通知套接字服务器关闭 */
void
socket_server_exit(struct socket_server *ss) {
	struct request_package request;
	send_request(ss, &request, 'X', 0);
}

/* 关闭套接字, 此函数会在写缓冲全部发送完之后才真的关闭套接字, 在此之前套接字将处于半关闭状态.
 * 此时将把写缓冲中的数据发送出去, 但是不能再向其中添加数据.
 *
 * 参数: ss 是套接字服务器; opaque 是服务句柄; id 是待关闭的套接字标识;
 * 函数无返回值, 会异步的通知套接字关闭 */
void
socket_server_close(struct socket_server *ss, uintptr_t opaque, int id) {
	struct request_package request;
	request.u.close.id = id;
	request.u.close.shutdown = 0;
	request.u.close.opaque = opaque;
	send_request(ss, &request, 'K', sizeof(request.u.close));
}

/* 立即关闭套接字, 此函数将尽可能快的关闭套接字. 如果套接字的写缓冲中还有数据, 将尽可能多的写入,
 * 但是即便没有完全写完也会尽快的关闭套接字.
 *
 * 参数: ss 是套接字服务器; opaque 是服务句柄; id 是待关闭的套接字标识;
 * 函数无返回值, 会异步的通知套接字关闭 */
void
socket_server_shutdown(struct socket_server *ss, uintptr_t opaque, int id) {
	struct request_package request;
	request.u.close.id = id;
	request.u.close.shutdown = 1;
	request.u.close.opaque = opaque;
	send_request(ss, &request, 'K', sizeof(request.u.close));
}

// return -1 means failed
// or return AF_INET or AF_INET6

/* 将 host 和 port 表示的地址以协议 protocol 绑定到一个操作系统的套接字上, 并返回此套接字的文件描述符.
 * 如果不指定 host 则为 INADDR_ANY, 套接字绑定到所有的系统网络接口上. 成功之后将重用此 bind 中的地址.
 *
 * 参数: host 是绑定的主机; port 是绑定的端口; protocol 是协议, 只支持 IPPROTO_TCP 和 IPPROTO_UDP;
 * 出参 family 用户包含最终成功的套接字的域, 为 AF_INET 或者 AF_INET6 .
 *
 * 返回: 成功后的文件描述符或者 -1 表示失败. */
static int
do_bind(const char *host, int port, int protocol, int *family) {
	int fd;
	int status;
	int reuse = 1;
	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	char portstr[16];
	if (host == NULL || host[0] == 0) {
		host = "0.0.0.0";	// INADDR_ANY
	}
	/* [ck]port 可以为 0 吗?[/ck] */
	sprintf(portstr, "%d", port);
	memset( &ai_hints, 0, sizeof( ai_hints ) );
	ai_hints.ai_family = AF_UNSPEC;
	if (protocol == IPPROTO_TCP) {
		ai_hints.ai_socktype = SOCK_STREAM;
	} else {
		assert(protocol == IPPROTO_UDP);
		ai_hints.ai_socktype = SOCK_DGRAM;
	}
	ai_hints.ai_protocol = protocol;

	status = getaddrinfo( host, portstr, &ai_hints, &ai_list );
	if ( status != 0 ) {
		return -1;
	}
	*family = ai_list->ai_family;
	fd = socket(*family, ai_list->ai_socktype, 0);
	if (fd < 0) {
		goto _failed_fd;
	}
	if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (void *)&reuse, sizeof(int))==-1) {
		goto _failed;
	}
	status = bind(fd, (struct sockaddr *)ai_list->ai_addr, ai_list->ai_addrlen);
	if (status != 0)
		goto _failed;

	freeaddrinfo( ai_list );
	return fd;
_failed:
	close(fd);
_failed_fd:
	freeaddrinfo( ai_list );
	return -1;
}

/* 对 host 和 port 指示的地址进行侦听并返回套接字的文件描述符.
 * 参数: host 是主机名; port 是端口号; backlog 是未完成连接的请求的队列大小;
 * 返回: 套接字的文件描述符, 在失败的情况下将返回 -1 . */
static int
do_listen(const char * host, int port, int backlog) {
	int family = 0;
	int listen_fd = do_bind(host, port, IPPROTO_TCP, &family);
	if (listen_fd < 0) {
		return -1;
	}
	if (listen(listen_fd, backlog) == -1) {
		close(listen_fd);
		return -1;
	}
	return listen_fd;
}

/* 对由 addr 和 port 指定的地址进行侦听.
 *
 * 参数: ss 是套接字服务器; opaque 是套接字所属的服务; addr 是主机名, 可以是 IPv4 和 IPv6 两种形式; port 是端口号;
 * backlog 是未完成连接的请求的队列大小;
 *
 * 返回: 侦听成功后的套接字 id , 如果失败将返回小于 0 的值. */
int 
socket_server_listen(struct socket_server *ss, uintptr_t opaque, const char * addr, int port, int backlog) {
	int fd = do_listen(addr, port, backlog);
	if (fd < 0) {
		return -1;
	}
	struct request_package request;
	int id = reserve_id(ss);
	if (id < 0) {
		close(fd);
		return id;
	}
	request.u.listen.opaque = opaque;
	request.u.listen.id = id;
	request.u.listen.fd = fd;
	send_request(ss, &request, 'L', sizeof(request.u.listen));
	return id;
}

/* 将一个操作系统套接字的文件描述符 fd 添加到 skynet 中, 并生成一个对应的套接字.
 * 参数: ss 是套接字服务器; opaque 是服务句柄; fd 是操作系统中的套接字文件描述符;
 * 返回: 生成的套接字 id */
int
socket_server_bind(struct socket_server *ss, uintptr_t opaque, int fd) {
	struct request_package request;
	int id = reserve_id(ss);
	if (id < 0)
		return -1;
	request.u.bind.opaque = opaque;
	request.u.bind.id = id;
	request.u.bind.fd = fd;
	send_request(ss, &request, 'B', sizeof(request.u.bind));
	return id;
}

/* 将处于 SOCKET_TYPE_PACCEPT 和 SOCKET_TYPE_PLISTEN 状态的套接字继续向前推进, 使得可以接收 I/O 事件通知.
 * 如果套接字处于 SOCKET_TYPE_CONNECTED 状态, 将被转移到别的服务中去. 此函数仅对以上三种状态的套接字有效.
 * 其它形式的套接字将不起作用. 函数的返回值以异步形式有 start_socket 函数返回.
 *
 * 参数: ss 是套接字服务器; opaque 是服务句柄; id 是套接字的标识符; */
void 
socket_server_start(struct socket_server *ss, uintptr_t opaque, int id) {
	struct request_package request;
	request.u.start.id = id;
	request.u.start.opaque = opaque;
	send_request(ss, &request, 'S', sizeof(request.u.start));
}

/* 设置套接字的非延迟属性. 函数无返回值. */
void
socket_server_nodelay(struct socket_server *ss, int id) {
	struct request_package request;
	request.u.setopt.id = id;
	request.u.setopt.what = TCP_NODELAY;
	request.u.setopt.value = 1;
	send_request(ss, &request, 'T', sizeof(request.u.setopt));
}

/* 设置套接字服务器使用 userobject , 一旦设置成功, 将调用 soi 接口中的函数来生成和销毁套接字写缓存. */
void 
socket_server_userobject(struct socket_server *ss, struct socket_object_interface *soi) {
	ss->soi = *soi;
}

// UDP
/* 生成一个 UDP 套接字, addr 和 port 中的任意都可以为 NULL 或 0 , 如果不为此值将会绑定到这两个值表示的地址上.
 * 参数: ss 是套接字服务器; opaque 是服务句柄; addr 是绑定的地址可以为 NULL ; port 是绑定的端口号可以为 0 ;
 * 返回: 生成的套接字的 id , 失败时返回 -1 . */
int 
socket_server_udp(struct socket_server *ss, uintptr_t opaque, const char * addr, int port) {
	int fd;
	int family;
	if (port != 0 || addr != NULL) {
		// bind
		fd = do_bind(addr, port, IPPROTO_UDP, &family);
		if (fd < 0) {
			return -1;
		}
	} else {
		family = AF_INET;
		fd = socket(family, SOCK_DGRAM, 0);
		if (fd < 0) {
			return -1;
		}
	}
	sp_nonblocking(fd);

	int id = reserve_id(ss);
	if (id < 0) {
		close(fd);
		return -1;
	}
	struct request_package request;
	request.u.udp.id = id;
	request.u.udp.fd = fd;
	request.u.udp.opaque = opaque;
	request.u.udp.family = family;

	send_request(ss, &request, 'U', sizeof(request.u.udp));	
	return id;
}

/* 向 UDP 套接字中的写入缓冲数据, 如果未提供发送的对端地址, 消息将不发送而是直接销毁. 如果不希望提供对端地址,
 * 需要先调用 socket_server_udp_connect 添加一个对端地址, 再调用 socket_server_send 发送数据.
 *
 * 参数: ss 是套接字服务器; id 是发送数据的套接字标识符; addr 是消息发送的对端地址; buffer 是消息内容缓冲; sz 是消息内容大小;
 * 返回: 套接字中的缓冲数据大小, 如果失败将返回 -1 . */
int64_t 
socket_server_udp_send(struct socket_server *ss, int id, const struct socket_udp_address *addr, const void *buffer, int sz) {
	struct socket * s = &ss->slot[HASH_ID(id)];
	if (s->id != id || s->type == SOCKET_TYPE_INVALID) {
		free_buffer(ss, buffer, sz);
		return -1;
	}

	struct request_package request;
	request.u.send_udp.send.id = id;
	request.u.send_udp.send.sz = sz;
	request.u.send_udp.send.buffer = (char *)buffer;
	
	/* 复制到请求体的地址中, 如果不存在相应的 type 或者地址为 NULL 将不能成功发送
	 * [ck]当 addr 为 NULL 时, 可以从套接字中取得地址[/ck] */
	const uint8_t *udp_address = (const uint8_t *)addr;
	int addrsz;
	switch (udp_address[0]) {
	case PROTOCOL_UDP:
		addrsz = 1+2+4;		// 1 type, 2 port, 4 ipv4
		break;
	case PROTOCOL_UDPv6:
		addrsz = 1+2+16;	// 1 type, 2 port, 16 ipv6
		break;
	default:
		free_buffer(ss, buffer, sz);
		return -1;
	}

	memcpy(request.u.send_udp.address, udp_address, addrsz);	

	send_request(ss, &request, 'A', sizeof(request.u.send_udp.send)+addrsz);
	return s->wb_size;
}

/* 发起 UDP 连接的请求. 其实质是给此套接字添加一个关联的地址, 并未调用系统的 connect 函数.
 * 参数: ss 是套接字服务器; id 是套接字的唯一标识; addr 是连接的对端地址; port 是对端端口号;
 * 返回: 成功时返回 0 , 失败时返回 -1 . */
int
socket_server_udp_connect(struct socket_server *ss, int id, const char * addr, int port) {
	int status;
	struct addrinfo ai_hints;
	struct addrinfo *ai_list = NULL;
	char portstr[16];
	sprintf(portstr, "%d", port);
	memset( &ai_hints, 0, sizeof( ai_hints ) );
	ai_hints.ai_family = AF_UNSPEC;
	ai_hints.ai_socktype = SOCK_DGRAM;
	ai_hints.ai_protocol = IPPROTO_UDP;

	status = getaddrinfo(addr, portstr, &ai_hints, &ai_list );
	if ( status != 0 ) {
		return -1;
	}
	struct request_package request;
	request.u.set_udp.id = id;
	int protocol;

	if (ai_list->ai_family == AF_INET) {
		protocol = PROTOCOL_UDP;
	} else if (ai_list->ai_family == AF_INET6) {
		protocol = PROTOCOL_UDPv6;
	} else {
		freeaddrinfo( ai_list );
		return -1;
	}

	int addrsz = gen_udp_address(protocol, (union sockaddr_all *)ai_list->ai_addr, request.u.set_udp.address);

	freeaddrinfo( ai_list );

	send_request(ss, &request, 'C', sizeof(request.u.set_udp) - sizeof(request.u.set_udp.address) +addrsz);

	return 0;
}

/* 从套接字消息中分离出套接字地址. 地址保存在消息的末端, 起始地址有 msg 中的 ud 来标记, ud 为接收到的消息的实际长度.
 * 参数: ss 是套接字服务器, 在此函数中未使用; msg 是查询条件; 出参 addrsz 用于保存套接字地址的长度;
 * 返回: 套接字地址的内存起始地址 */
const struct socket_udp_address *
socket_server_udp_address(struct socket_server *ss, struct socket_message *msg, int *addrsz) {
	uint8_t * address = (uint8_t *)(msg->data + msg->ud);
	int type = address[0];
	switch(type) {
	case PROTOCOL_UDP:
		*addrsz = 1+2+4;
		break;
	case PROTOCOL_UDPv6:
		*addrsz = 1+2+16;
		break;
	default:
		return NULL;
	}
	return (const struct socket_udp_address *)address;
}
