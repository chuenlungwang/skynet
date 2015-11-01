#ifndef SKYNET_MESSAGE_QUEUE_H
#define SKYNET_MESSAGE_QUEUE_H

#include <stdlib.h>
#include <stdint.h>

/* 消息结构 */
struct skynet_message {
	uint32_t source;     /* 发送消息的服务 id */
	int session;         /* 会话号, 发送请求消息时会分配一个唯一会话号, 发送回复消息时必须与
	                        请求消息的会话号一样, 用于 skynet 查找相应处理逻辑 */
	void * data;         /* 消息内容指针, 通常是发送方分配内存, 接收函数销毁内存 */
	size_t sz;           /* 高8位保存了消息类型, 剩下的低位保存了消息内容大小 */
};

// type is encoding in skynet_message.sz high 8bit
#define MESSAGE_TYPE_MASK (SIZE_MAX >> 8)
#define MESSAGE_TYPE_SHIFT ((sizeof(size_t)-1) * 8)

/* 单个服务拥有的消息队列, 在 skynet_mq 模块外边并不直接访问其内部结构, 因而不需要暴露字段 */
struct message_queue;

void skynet_globalmq_push(struct message_queue * queue);
struct message_queue * skynet_globalmq_pop(void);

struct message_queue * skynet_mq_create(uint32_t handle);
void skynet_mq_mark_release(struct message_queue *q);

/* 销毁消息的函数类型声明, 第二个参数为自定义的参数 */
typedef void (*message_drop)(struct skynet_message *, void *);

void skynet_mq_release(struct message_queue *q, message_drop drop_func, void *ud);
uint32_t skynet_mq_handle(struct message_queue *);

// 0 for success
int skynet_mq_pop(struct message_queue *q, struct skynet_message *message);
void skynet_mq_push(struct message_queue *q, struct skynet_message *message);

// return the length of message queue, for debug
int skynet_mq_length(struct message_queue *q);
int skynet_mq_overload(struct message_queue *q);

void skynet_mq_init();

#endif
