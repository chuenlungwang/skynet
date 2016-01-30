#ifndef skynet_hashid_h
#define skynet_hashid_h

#include <assert.h>
#include <stdlib.h>
#include <string.h>

/* 哈希表中一个节点 */
struct hashid_node {
	int id;                       /* 哈希表中的键, 最终得到的值为此哈希节点在哈希数组中的索引 */
	struct hashid_node *next;     /* 哈希值相同的链表中的下一个元素, 位于末端或者没有被使用的哈希节点的 next 为 NULL */
};

/* 哈希表结构, 其哈希键存储在节点 hashid_node 的 id 字段中, 值为节点在数组 id 中的索引.
 * 实际的哈希表为 hash , 由 id & hashmod 得到在 hash 中位置, 依次访问链表中的每个节点比较键是否相同. */
struct hashid {
	int hashmod;                   /* 哈希的模值, 其为大于 cap 的最小的二的幂减 1 , 目的是保证模值的所有位都是 1 */
	int cap;                       /* 哈希的容量, 为 id 数组的大小 */
	int count;                     /* 已经使用的哈希节点的数量, 占用掉的哈希节点都会挂载到 hash 表中 */
	struct hashid_node *id;        /* 所有哈希节点组成的数组, 同时哈希键对应的值为哈希节点在数组中的索引 */
	struct hashid_node **hash;     /* 真正的哈希表, 具有相同哈希值的哈希节点链接在一起, 未使用的哈希值的链表为 NULL */
};

/* 哈希表初始化, 将分配哈希节点数组以及内部哈希表数组的内存.
 * 内部哈希数组的内存大小是大于 max 的最小的二的幂减 1 , 目的是保证模值的所有位都是 1 .
 *
 * 参数: hi 是哈希表结构; max 是节点的容量; */
static void
hashid_init(struct hashid *hi, int max) {
	int i;
	int hashcap;
	hashcap = 16;
	while (hashcap < max) {
		hashcap *= 2;
	}
	hi->hashmod = hashcap - 1;
	hi->cap = max;
	hi->count = 0;
	hi->id = skynet_malloc(max * sizeof(struct hashid_node));
	for (i=0;i<max;i++) {
		hi->id[i].id = -1;
		hi->id[i].next = NULL;
	}
	hi->hash = skynet_malloc(hashcap * sizeof(struct hashid_node *));
	memset(hi->hash, 0, hashcap * sizeof(struct hashid_node *));
}

/* 清理哈希表, 具体是将哈希节点数组和哈希表数组的内存释放, 并将其它的值置为初始值 . */
static void
hashid_clear(struct hashid *hi) {
	skynet_free(hi->id);
	skynet_free(hi->hash);
	hi->id = NULL;
	hi->hash = NULL;
	hi->hashmod = 1;
	hi->cap = 0;
	hi->count = 0;
}

/* 在哈希表中查找键 id 对应的哈希节点的索引. 如果没有找到就返回 -1 .
 * 整个过程首先是对 hashmod 位与运算得到哈希列表, 然后比较哈希列表中所有的节点是否 id 相同,
 * 若相同就返回其在 id 数组索引. */
static int
hashid_lookup(struct hashid *hi, int id) {
	int h = id & hi->hashmod;
	struct hashid_node * c = hi->hash[h];
	while(c) {
		if (c->id == id)
			return c - hi->id;
		c = c->next;
	}
	return -1;
}

/* 从哈希表中移除键为 id 对应的哈希值. 首先需要找到此哈希节点, 若找到则从哈希列表中移除,
 * 并将其 id 重新置为 -1 . 移除成功则返回原先的节点在数组 id 中的索引, 如果没有此哈希则返回 -1 . */
static int
hashid_remove(struct hashid *hi, int id) {
	int h = id & hi->hashmod;
	struct hashid_node * c = hi->hash[h];
	if (c == NULL)
		return -1;
	if (c->id == id) {
		hi->hash[h] = c->next;
		goto _clear;
	}
	while(c->next) {
		if (c->next->id == id) {
			struct hashid_node * temp = c->next;
			c->next = temp->next;
			c = temp;
			goto _clear;
		}
		c = c->next;
	}
	return -1;
_clear:
	c->id = -1;
	c->next = NULL;
	--hi->count;
	return c - hi->id;
}

/* 将键 id 插入到哈希表中, 寻找哈希节点的过程是在哈希节点数组中寻找离键 id 最近的节点,
 * 如果未能找到则继续下一个节点, 直到找到. 此函数要保证能够完成插入工作, 即哈希表未满.
 * 调用 hashid_full 可以检测哈希表是否已经满了. 取得节点之后将把此节点插入到索引为哈希值 id & hashmod
 * 的哈希列表的头部, 并返回节点在哈希节点数组中索引. */
static int
hashid_insert(struct hashid * hi, int id) {
	struct hashid_node *c = NULL;
	int i;
	for (i=0;i<hi->cap;i++) {
		int index = (i+id) % hi->cap;
		if (hi->id[index].id == -1) {
			c = &hi->id[index];
			break;
		}
	}
	assert(c);
	++hi->count;
	c->id = id;
	assert(c->next == NULL);
	int h = id & hi->hashmod;
	if (hi->hash[h]) {
		c->next = hi->hash[h];
	}
	hi->hash[h] = c;
	
	return c - hi->id;
}

/* 检查哈希表是否已经满员了, 当插入节点时哈希表的空闲节点将减少, 而移除节点将增加. */
static inline int
hashid_full(struct hashid *hi) {
	return hi->count == hi->cap;
}

#endif
