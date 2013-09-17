/*
 * timededup.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef TIMEDEDUP_H_
#define TIMEDEDUP_H_

#include <stdlib.h>
#include <stdint.h>
#include <linux_list.h>
#include <pthread.h>

#define SEG_SIZE 131072

#define FP_SIZE 20
// #define FP_ZERO "\xc5\x0b\x60\xfa\x71\x0a\xa9\x4f\x3e\x56\x15\xb4\xd4\xa1\x94\xe3\xcd\xfa\xcf\x95\xe0\x25\x3b\x6a"
#define FP_ZERO "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d"
#define FP_IS_ZERO(fp) (!memcmp(fp, FP_ZERO, FP_SIZE))

#define STRING_BUF_SIZE 128

//#define SEGMENT_PREFETCH_AHEAD (524288U * 4096 / SEG_SIZE)
#define SEGMENT_PREFETCH_AHEAD 8192


typedef struct hlist_head hlist_head;
typedef struct hlist_node hlist_node;

typedef struct {
		hlist_head head;
		pthread_spinlock_t lock;
} HashHead;

#endif /* TIMEDEDUP_H_ */
