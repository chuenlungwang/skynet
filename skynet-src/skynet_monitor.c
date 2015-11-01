#include "skynet.h"

#include "skynet_monitor.h"
#include "skynet_server.h"
#include "skynet.h"
#include "atomic.h"

#include <stdlib.h>
#include <string.h>

/* skynet_monitor 模块用于检测工作线程是否陷入了死循环中. 每次触发监控时 struct skynet_monitor 结构
 * 中的 version 都会以线程安全的方式自增 1 , 当每次检测时会对比 version 和 check_version 是否相同, 若
 * 相同则表明有可能陷入了死循环, 若不相同则将 check_version 设置为与 version 值相同. 检查函数在单独一条
 * monitor 线程中调用, 因而不会有并发的可能性. 一个监控对象对应一条工作线程. */

/* 监视信息结构体 */
struct skynet_monitor {
	int version;               /* 当前版本号 */
	int check_version;         /* 上次检查时的版本号 */
	uint32_t source;           /* 发送消息的服务 id */
	uint32_t destination;      /* 接收消息的服务 id */
};

/* 在堆中构造一个 struct skynet_monitor 对象, 并执行初始化.
 * 调用者使用完之后必须调用 skynet_monitor_delete 进行回收. */
struct skynet_monitor * 
skynet_monitor_new() {
	struct skynet_monitor * ret = skynet_malloc(sizeof(*ret));
	memset(ret, 0, sizeof(*ret));
	return ret;
}

/* 回收 struct skynet_monitor 对象 */
void 
skynet_monitor_delete(struct skynet_monitor *sm) {
	skynet_free(sm);
}

/* 触发一次监控, 并对此次监控生成一个唯一的版本号. struct skynet_monitor 对象复用之前的, 不必每次生成新对象.
 * 此函数是线程安全的. */
void 
skynet_monitor_trigger(struct skynet_monitor *sm, uint32_t source, uint32_t destination) {
	sm->source = source;
	sm->destination = destination;
	ATOM_INC(&sm->version);
}

/* 检查监控对象所监控的消息处理过程是否陷入死循环. 其实现原理是检查当前版本号与上次检查时的版本号是否相同. 
 * 若相同, 且当前确实有消息在处理(destination 字段所代表的接收服务 id 不为 0), 则标记接收陷入死循环,
 * 并发出日志警告. 如果不相同, 则更新检查的版本号为当前版本号. */
void 
skynet_monitor_check(struct skynet_monitor *sm) {
	if (sm->version == sm->check_version) {
		if (sm->destination) {
			skynet_context_endless(sm->destination);
			skynet_error(NULL, "A message from [ :%08x ] to [ :%08x ] maybe in an endless loop (version = %d)", sm->source , sm->destination, sm->version);
		}
	} else {
		sm->check_version = sm->version;
	}
}
