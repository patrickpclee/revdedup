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

#define BLOCK_SIZE 4096
//#define SEG_BLOCKS 1024
//#define SEG_BLOCKS 2048
//#define SEG_BLOCKS 4096
#define SEG_BLOCKS 8192
#define SEG_SIZE (BLOCK_SIZE * SEG_BLOCKS)

#define PORT 5749

#define FP_SIZE 20
#define FP_ZERO "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d"
#define FP_IS_ZERO(fp) (!memcmp(fp, FP_ZERO, FP_SIZE))

#define STRING_BUF_SIZE 128

#define SEGMENT_PREFETCH_AHEAD (524288 / SEG_BLOCKS)

typedef struct hlist_head hlist_head;
typedef struct hlist_node hlist_node;

typedef struct {
		hlist_head head;
		pthread_spinlock_t lock;
} HashHead;

#endif /* TIMEDEDUP_H_ */
