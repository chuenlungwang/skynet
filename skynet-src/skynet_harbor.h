#ifndef SKYNET_HARBOR_H
#define SKYNET_HARBOR_H

#include <stdint.h>
#include <stdlib.h>

#define GLOBALNAME_LENGTH 16
#define REMOTE_MAX 256

/* 远程服务的名字或者地址, 使用时仅适用其中一个 */
struct remote_name {
	char name[GLOBALNAME_LENGTH];     /* 服务的全局名字, 限制在 GLOBALNAME_LENGTH 长度内 */
	uint32_t handle;                  /* 远程服务的地址 */
};

/* 远程消息结构, 此消息会被发送到其他的 skynet 节点 */
struct remote_message {
	struct remote_name destination;   /* 接收消息的服务, 为一个全局名字或者服务地址 */
	const void * message;             /* 消息内容, 存在于堆内存中 */
	size_t sz;                        /* 消息大小 */
};

void skynet_harbor_send(struct remote_message *rmsg, uint32_t source, int session);
int skynet_harbor_message_isremote(uint32_t handle);
void skynet_harbor_init(int harbor);
void skynet_harbor_start(void * ctx);
void skynet_harbor_exit();

#endif
