#include "skynet.h"
#include "skynet_harbor.h"
#include "skynet_server.h"
#include "skynet_mq.h"
#include "skynet_handle.h"

#include <string.h>
#include <stdio.h>
#include <assert.h>

static struct skynet_context * REMOTE = 0;

/* ~0 是一个不可能是 harbor 值, 而 0 是系统保留的 harbor 值 */
static unsigned int HARBOR = ~0;

/* 向远程服务发送一个远程消息 rmsg , 其中 rmsg 存在于堆内存中, 由此函数负责释放其内存.
 * 参数: rmsg 是将被发送的远程消息, source 是发送消息的服务地址, session 是当前消息的会话 id
 * 此函数无返回值 */
void 
skynet_harbor_send(struct remote_message *rmsg, uint32_t source, int session) {
	/* 还原出消息类型和消息内容的大小 */
	int type = rmsg->sz >> MESSAGE_TYPE_SHIFT;
	rmsg->sz &= MESSAGE_TYPE_MASK;
	
	/* PTYPE_HARBOR 和 PTYPE_SYSTEM 都是保留给系统使用的, 上层服务只使用其它的消息类型 */
	assert(type != PTYPE_SYSTEM && type != PTYPE_HARBOR && REMOTE);
	skynet_context_send(REMOTE, rmsg, sizeof(*rmsg) , source, type , session);
}

/* 检查一个服务地址是否是远程服务地址.
 * 参数: handle 是查询的服务地址
 * 返回: 1 表示确实是远程服务, 0 表示不是远程服务 */
int 
skynet_harbor_message_isremote(uint32_t handle) {
	assert(HARBOR != ~0);
	int h = (handle & ~HANDLE_MASK);
	return h != HARBOR && h !=0;
}

/* 初始化 harbor 模块, 方法很简单就是将 harbor 值移动到最高 8 位.
 * 参数: harbor 为当前节点的 id, 要求值的范围在 255 以内
 * 此函数无返回值 */
void
skynet_harbor_init(int harbor) {
	HARBOR = (unsigned int)harbor << HANDLE_REMOTE_SHIFT;
}

/* 在启动 harbor 服务时调用, 注册 harbor 服务到 harbor 模块中, 之后所有的消息发送都会被转发到 harbor 服务中.
 * 此函数会将服务 ctx 从 skynet_server 中保留出来, 被保留的服务保证当调用 skynet_handle_retire 并不真正退出服务,
 * 而是最后手动调用释放掉服务.
 *
 * 参数: ctx 是 harbor 服务
 * 此函数无返回值 */
void
skynet_harbor_start(void *ctx) {
	// the HARBOR must be reserved to ensure the pointer is valid.
	// It will be released at last by calling skynet_harbor_exit
	skynet_context_reserve(ctx);
	REMOTE = ctx;
}

/* 退出 harbor 模块, 此函数会同时释放掉 harbor 服务. */
void
skynet_harbor_exit() {
	struct skynet_context * ctx = REMOTE;
	REMOTE= NULL;
	if (ctx) {
		skynet_context_release(ctx);
	}
}
