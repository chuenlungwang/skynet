#ifndef SKYNET_ATOMIC_H
#define SKYNET_ATOMIC_H

/* 用于比较交换整数的原子操作, 当交换成功时返回 true */
#define ATOM_CAS(ptr, oval, nval) __sync_bool_compare_and_swap(ptr, oval, nval)

/* ATOM_CAS_POINTER 专门用于比较交换指针的原子操作, 当交换成功时返回 true */
#define ATOM_CAS_POINTER(ptr, oval, nval) __sync_bool_compare_and_swap(ptr, oval, nval)
#define ATOM_INC(ptr) __sync_add_and_fetch(ptr, 1)
#define ATOM_FINC(ptr) __sync_fetch_and_add(ptr, 1)
#define ATOM_DEC(ptr) __sync_sub_and_fetch(ptr, 1)
#define ATOM_FDEC(ptr) __sync_fetch_and_sub(ptr, 1)
#define ATOM_ADD(ptr,n) __sync_add_and_fetch(ptr, n)
#define ATOM_SUB(ptr,n) __sync_sub_and_fetch(ptr, n)

/* 对 ptr 所指向的变量和值 n 做原子位与操作, 结果保存在 ptr 指向的变量中并返回结果 */
#define ATOM_AND(ptr,n) __sync_and_and_fetch(ptr, n)

#endif
