#ifndef SKYNET_H
#define SKYNET_H

#include "skynet_malloc.h"

#include <stddef.h>
#include <stdint.h>

/* 消息类型被分配在 type 的低 8 位, 在发送消息时需要嵌入到 size 的高 8 位,
 * PTYPE_TAG_ALLOCSESSION 和 PTYPE_TAG_DONTCOPY 则在高位每个单独占据一位,
 * 这两个标记在调用 skynet_send 时嵌入到消息类型 type 中, 指示是否需要复制信息和分配会话 id */
#define PTYPE_TEXT 0
#define PTYPE_RESPONSE 1
#define PTYPE_MULTICAST 2
#define PTYPE_CLIENT 3
#define PTYPE_SYSTEM 4
#define PTYPE_HARBOR 5
#define PTYPE_SOCKET 6
// read lualib/skynet.lua examples/simplemonitor.lua
#define PTYPE_ERROR 7	
// read lualib/skynet.lua lualib/mqueue.lua lualib/snax.lua
#define PTYPE_RESERVED_QUEUE 8
#define PTYPE_RESERVED_DEBUG 9
#define PTYPE_RESERVED_LUA 10
#define PTYPE_RESERVED_SNAX 11

#define PTYPE_TAG_DONTCOPY 0x10000
#define PTYPE_TAG_ALLOCSESSION 0x20000

struct skynet_context;

/* 本文件指示所有散布于 skynet-src 中的 skynet 开头的函数, 并且暴露给上层服务使用,
 * 而其它的方法如 skynet_context_* skynet_mq_* skynet_handle_* 等方法基本上是由 skynet 内部模块使用的. */

void skynet_error(struct skynet_context * context, const char *msg, ...);
const char * skynet_command(struct skynet_context * context, const char * cmd , const char * parm);
uint32_t skynet_queryname(struct skynet_context * context, const char * name);
int skynet_send(struct skynet_context * context, uint32_t source, uint32_t destination , int type, int session, void * msg, size_t sz);
int skynet_sendname(struct skynet_context * context, uint32_t source, const char * destination , int type, int session, void * msg, size_t sz);

int skynet_isremote(struct skynet_context *, uint32_t handle, int * harbor);

typedef int (*skynet_cb)(struct skynet_context * context, void *ud, int type, int session, uint32_t source , const void * msg, size_t sz);
void skynet_callback(struct skynet_context * context, void *ud, skynet_cb cb);

uint32_t skynet_current_handle(void);
uint64_t skynet_now(void);
void skynet_debug_memory(const char *info);	// for debug use, output current service memory to stderr

#endif
