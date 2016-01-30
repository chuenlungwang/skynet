#ifndef skynet_databuffer_h
#define skynet_databuffer_h

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define MESSAGEPOOL 1023

/* 消息链表中的消息节点 */
struct message {
	char * buffer;            /* 消息内容指针, 为一块堆内存 */
	int size;                 /* 消息内容大小 */
	struct message * next;    /* 位于消息队列中的下一个节点, 若此节点为最后一个节点 next 为 NULL */
};

/* 数据缓存, 其实质就是一个消息链表, 一条数据可能跨越多个消息, 并且每条数据的前端都有一个 2 字节
 * 或者 4 字节的头, 以大端形式构成数据的长度 */
struct databuffer {
	int header;               /* 数据头, 为一条数据的大小, 将从数据缓存中读出, 当读完数据之后应清 0 */
	int offset;               /* 在当前头节点消息中读取数据的起点, 因为有时读取数据并不是一整个消息 */
	int size;                 /* 所有数据的大小 */
	struct message * head;    /* 数据缓冲的头结点, 初始时为 NULL */
	struct message * tail;    /* 数据缓冲的尾节点, 初始时为 NULL */
};

/* 保存缓存消息的消息池队列节点, 每次分配一个 MESSAGEPOOL 大小的消息数组, 衔接成队列,
 * 分配好的数组中的消息连接起来全部放入到池子中的 freelist 中, 每使用一个消息就取走一个,
 * 每归还一个消息就放入到 freelist 的头部 */
struct messagepool_list {
	struct messagepool_list *next;       /* 位于消息池的下一个节点, 若此节点为最后一个节点 next 为 NULL */
	struct message pool[MESSAGEPOOL];    /* 消息数组, 分配好内存后将放入 freelist 中, 供使用 */
};

/* 保存缓存消息的消息池, 所有消息的内存都来自于 pool , 并且在未使用时都放在 freelist 中, 需要时就从中取得
 * 使用完之后归还到其中. 如果不够的话将继续分配 pool . */
struct messagepool {
	struct messagepool_list * pool;       /* 消息池队列头节点, 初始化时为 NULL */
	struct message * freelist;            /* 消息池空闲列表, 初始化时为 NULL */
};

// use memset init struct 

/* 释放消息池的内存, 先销毁整个消息池队列, 再将它们重设为 NULL */
static void 
messagepool_free(struct messagepool *pool) {
	struct messagepool_list *p = pool->pool;
	while(p) {
		struct messagepool_list *tmp = p;
		p=p->next;
		skynet_free(tmp);
	}
	pool->pool = NULL;
	pool->freelist = NULL;
}

/* 将数据缓存中的头结点重新放回消息池中, 供下次使用. 当全部归还之后 head 和 tail 将重置为 NULL . */
static inline void
_return_message(struct databuffer *db, struct messagepool *mp) {
	struct message *m = db->head;
	if (m->next == NULL) {
		assert(db->tail == m);
		db->head = db->tail = NULL;
	} else {
		db->head = m->next;
	}
	skynet_free(m->buffer);
	m->buffer = NULL;
	m->size = 0;
	m->next = mp->freelist;
	mp->freelist = m;
}

/* 从数据缓存中读取大小为 sz 大小的数据, 并由足够大的 buffer 接收数据, 读取完数据之后的消息将
 * 归还到消息池中.
 *
 * 参数: db 是数据缓冲; mp 是消息池, 读取后消息会归还此处, 写入将从此处取走空闲消息;
 * 出参 buffer 接收读取到的数据; sz 是要读取的数据的大小; */
static void
databuffer_read(struct databuffer *db, struct messagepool *mp, void * buffer, int sz) {
	assert(db->size >= sz);
	db->size -= sz;
	/* 读取的数据量可能跨越多个消息 */
	for (;;) {
		struct message *current = db->head;
		int bsz = current->size - db->offset;
		if (bsz > sz) {
			memcpy(buffer, current->buffer + db->offset, sz);
			db->offset += sz;
			return;
		}
		if (bsz == sz) {
			memcpy(buffer, current->buffer + db->offset, sz);
			db->offset = 0;
			_return_message(db, mp);
			return;
		} else {
			memcpy(buffer, current->buffer + db->offset, bsz);
			_return_message(db, mp);
			db->offset = 0;
			buffer+=bsz;
			sz-=bsz;
		}
	}
}

/* 将数据 data 置入数据缓存中去, 并放到缓存队列的尾部. 这将从消息池中取得一个消息用于装载数据. */
static void
databuffer_push(struct databuffer *db, struct messagepool *mp, void *data, int sz) {
	struct message * m;
	if (mp->freelist) {
		m = mp->freelist;
		mp->freelist = m->next;
	} else {
		/* 如果消息池中没有了空闲消息时, 将分配更多的消息数组 */
		struct messagepool_list * mpl = skynet_malloc(sizeof(*mpl));
		struct message * temp = mpl->pool;
		int i;
		for (i=1;i<MESSAGEPOOL;i++) {
			temp[i].buffer = NULL;
			temp[i].size = 0;
			temp[i].next = &temp[i+1];
		}
		temp[MESSAGEPOOL-1].next = NULL;
		mpl->next = mp->pool;
		mp->pool = mpl;
		m = &temp[0];
		mp->freelist = &temp[1];
	}
	m->buffer = data;
	m->size = sz;
	m->next = NULL;
	db->size += sz;
	if (db->head == NULL) {
		assert(db->tail == NULL);
		db->head = db->tail = m;
	} else {
		db->tail->next = m;
		db->tail = m;
	}
}

/* 从数据缓存中读取数据头部, 这个头部是一个大端排序的 2 字节或 4 字节的整数, 表示数据的长度.
 * 当读取过一次之后, 此头部会缓存在 db 的 header 字段中, 需要调用 databuffer_reset 重设为 0 .
 * 当读取失败时将返回 -1 .
 *
 * 参数: db 是数据缓冲; mp 是消息池, 用于回收读取过的消息; header_size 是头部大小;
 * 返回: 头部的大端形式解析出来的整数, 如果获取失败将返回 -1 . */
static int
databuffer_readheader(struct databuffer *db, struct messagepool *mp, int header_size) {
	if (db->header == 0) {
		// parser header (2 or 4)
		if (db->size < header_size) {
			return -1;
		}
		uint8_t plen[4];
		databuffer_read(db,mp,(char *)plen,header_size);
		// big-endian
		if (header_size == 2) {
			db->header = plen[0] << 8 | plen[1];
		} else {
			db->header = plen[0] << 24 | plen[1] << 16 | plen[2] << 8 | plen[3];
		}
	}
	if (db->size < db->header)
		return -1;
	return db->header;
}

/* 将读取过的头部重新置为 0 , 以免下一次调用读取数据头部读到上次的缓存值. */
static inline void
databuffer_reset(struct databuffer *db) {
	db->header = 0;
}

/* 清理掉数据缓存中的所有数据, 所有的消息都归还到消息池中, 且数据内存都被销毁.
 * 并且数据缓存对象也重设为初始信息. */
static void
databuffer_clear(struct databuffer *db, struct messagepool *mp) {
	while (db->head) {
		_return_message(db,mp);
	}
	memset(db, 0, sizeof(*db));
}

#endif
