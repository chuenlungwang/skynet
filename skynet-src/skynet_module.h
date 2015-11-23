#ifndef SKYNET_MODULE_H
#define SKYNET_MODULE_H

struct skynet_context;

typedef void * (*skynet_dl_create)(void);

/* 初始化服务实例的方法.
 * 参数: inst 是服务实例, struct skynet_context 是服务对象, parm 是初始化所需的参数
 * 返回: 0 表示初始化成功, 1 表示初始化失败. */
typedef int (*skynet_dl_init)(void * inst, struct skynet_context *, const char * parm);
typedef void (*skynet_dl_release)(void * inst);
typedef void (*skynet_dl_signal)(void * inst, int signal);

/* skynet 服务模块, 它们是一些动态链接库 */
struct skynet_module {
	const char * name;            /* 服务模块的动态链接库的名字, 除去后缀 so */
	void * module;                /* 动态链接库的句柄 */
	skynet_dl_create create;      /* 服务模块中的创建服务实例方法, 返回服务实例指针 */
	skynet_dl_init init;          /* 服务模块中的初始化服务实例方法, 返回 0 表示初始化成功, 1 表示初始化失败 */
	skynet_dl_release release;    /* 服务模块中卸载服务实例的方法 */
	skynet_dl_signal signal;      /* 服务模块中向服务实例发送信号的方法 */
};

void skynet_module_insert(struct skynet_module *mod);
struct skynet_module * skynet_module_query(const char * name);
void * skynet_module_instance_create(struct skynet_module *);
int skynet_module_instance_init(struct skynet_module *, void * inst, struct skynet_context *ctx, const char * parm);
void skynet_module_instance_release(struct skynet_module *, void *inst);
void skynet_module_instance_signal(struct skynet_module *, void *inst, int signal);

void skynet_module_init(const char *path);

#endif
