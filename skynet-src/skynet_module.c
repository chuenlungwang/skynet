#include "skynet.h"

#include "skynet_module.h"
#include "spinlock.h"

#include <assert.h>
#include <string.h>
#include <dlfcn.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

/* 最多只能容纳 MAX_MODULE_TYPE 个服务模块 */
#define MAX_MODULE_TYPE 32

/* 所有服务模块的收集器结构 */
struct modules {
	int count;                                  /* 当前已经启动的模块数 */
	struct spinlock lock;                       /* 并发操作收集器时需要加锁 */
	const char * path;                          /* 模块 so 文件所在目录模式 */
	struct skynet_module m[MAX_MODULE_TYPE];    /* 保存服务模块结构的数组 */
};

static struct modules * M = NULL;

/* 尝试打开名字为 name 的服务模块, 并将其保存于管理器 m 中. 目录模式 path 用分号分割, 并用 name
 * 替换其中每一个问号依次查询服务模块, 直到找到或者最终无法找到为止.
 * 参数: m 是模块管理器, name 是服务模块的名字.
 * 返回: 服务模块的指针, 如果不存此服务模块返回 NULL . */
static void *
_try_open(struct modules *m, const char * name) {
	const char *l;
	const char * path = m->path;
	size_t path_size = strlen(path);
	size_t name_size = strlen(name);

	int sz = path_size + name_size;
	//search path
	void * dl = NULL;
	char tmp[sz];
	do
	{
		/* 以分号为分隔符, 以问号为替换符, 得到所有的服务模块 so 文件地址. */
		memset(tmp,0,sz);
		while (*path == ';') path++;
		if (*path == '\0') break;
		l = strchr(path, ';');
		if (l == NULL) l = path + strlen(path);
		int len = l - path;
		int i;
		for (i=0;path[i]!='?' && i < len ;i++) {
			tmp[i] = path[i];
		}
		memcpy(tmp+i,name,name_size);
		if (path[i] == '?') {
			strncpy(tmp+i+name_size,path+i+1,len - i - 1);
		} else {
			fprintf(stderr,"Invalid C service path\n");
			exit(1);
		}
		dl = dlopen(tmp, RTLD_NOW | RTLD_GLOBAL);
		path = l;
	}while(dl == NULL);

	if (dl == NULL) {
		fprintf(stderr, "try open %s failed : %s\n",name,dlerror());
	}

	return dl;
}

/* 在管理器重查询名字为 name 的服务模块. 如果存在此模块就返回其模块指针, 否则返回 NULL .
 * 参数: name 为模块的名字
 * 返回: 模块的地址. */
static struct skynet_module * 
_query(const char * name) {
	int i;
	for (i=0;i<M->count;i++) {
		if (strcmp(M->m[i].name,name)==0) {
			return &M->m[i];
		}
	}
	return NULL;
}

/* 查询服务模块 mod 中的 4 个生命周期函数, 并保存在相应的模块字段中. 如果相应的函数不存在, 其字段将设置为
 * NULL. 函数返回 init 函数是否为 NULL .
 * 参数: mod 是服务模块.
 * 返回: 0 表示服务模块的 init 函数不为 NULL, 1 表示 init 函数为 NULL. */
static int
_open_sym(struct skynet_module *mod) {
	size_t name_size = strlen(mod->name);
	char tmp[name_size + 9]; // create/init/release/signal , longest name is release (7)
	memcpy(tmp, mod->name, name_size);
	strcpy(tmp+name_size, "_create");
	mod->create = dlsym(mod->module, tmp);
	strcpy(tmp+name_size, "_init");
	mod->init = dlsym(mod->module, tmp);
	strcpy(tmp+name_size, "_release");
	mod->release = dlsym(mod->module, tmp);
	strcpy(tmp+name_size, "_signal");
	mod->signal = dlsym(mod->module, tmp);

	return mod->init == NULL;
}

/* 查询名字 name 所代表的服务模块并返回服务模块指针. 如果服务模块还没有加载,
 * 就将其加载到管理器中再返回服务模块指针.
 * 参数: name 是服务模块的名字. */
struct skynet_module * 
skynet_module_query(const char * name) {
	struct skynet_module * result = _query(name);
	if (result)
		return result;

	SPIN_LOCK(M)

	result = _query(name); // double check

	if (result == NULL && M->count < MAX_MODULE_TYPE) {
		int index = M->count;
		void * dl = _try_open(M,name);
		if (dl) {
			M->m[index].name = name;
			M->m[index].module = dl;

			if (_open_sym(&M->m[index]) == 0) {
				M->m[index].name = skynet_strdup(name);
				M->count ++;
				result = &M->m[index];
			}
		}
	}

	SPIN_UNLOCK(M)

	return result;
}

/* 向管理器中插入服务模块 mod , 要求此服务模块之前不存在与管理器中.
 * 参数: mod 是服务模块
 * 此函数没有返回值. */
void 
skynet_module_insert(struct skynet_module *mod) {
	SPIN_LOCK(M)

	struct skynet_module * m = _query(mod->name);
	assert(m == NULL && M->count < MAX_MODULE_TYPE);
	int index = M->count;
	M->m[index] = *mod;
	++M->count;

	SPIN_UNLOCK(M)
}

/* 创建一个模块 m 的服务实例. 如果模块无法创建实例, 将返回最大无符号整形作为服务地址.
 * 服务模块最终会在初始化和释放服务时对此地址进行合理解释和利用.
 * 参数: m 为服务模块
 * 返回: 服务实例的地址 */
void * 
skynet_module_instance_create(struct skynet_module *m) {
	if (m->create) {
		return m->create();
	} else {
		return (void *)(intptr_t)(~0);
	}
}

/* 初始化服务实例. 传递给服务实例所属的服务对象 ctx 以及相关的初始化参数, 每个服务模块都必须有初始化方法.
 * 参数: m 是服务实例所属的服务模块, inst 是服务实例, ctx 是服务对象, parm 是初始化参数.
 * 返回: 初始化成功时返回 0 , 失败时返回 1 . */
int
skynet_module_instance_init(struct skynet_module *m, void * inst, struct skynet_context *ctx, const char * parm) {
	return m->init(inst, ctx, parm);
}

/* 释放服务实例 inst , 如果服务模块没有释放函数将不做任何操作.
 * 参数: m 是服务实例所属的服务模块, inst 是服务实例.
 * 此函数没有返回值. */
void 
skynet_module_instance_release(struct skynet_module *m, void *inst) {
	if (m->release) {
		m->release(inst);
	}
}

/* 向服务实例 inst 发送信号, 如果服务没有发送信号的函数将不做任何操作.
 * 参数: m 是服务实例所属的服务模块, inst 是服务实例, signal 是信号.
 * 此函数没有返回值. */
void
skynet_module_instance_signal(struct skynet_module *m, void *inst, int signal) {
	if (m->signal) {
		m->signal(inst, signal);
	}
}

/* 对服务模块管理器进行初始化.
 * 参数: path 是服务模块所在的路径
 * 此函数无返回值. */
void 
skynet_module_init(const char *path) {
	struct modules *m = skynet_malloc(sizeof(*m));
	m->count = 0;
	m->path = skynet_strdup(path);

	SPIN_INIT(m)

	M = m;
}
