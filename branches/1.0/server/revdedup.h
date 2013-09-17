/*
 * revdedup.h
 *
 *  Created on: Dec 10, 2012
 *      Author: chng
 */

#ifndef REVDEDUP_H_
#define REVDEDUP_H_

#include "rdserver.h"
#include <pthread.h>
#include "storage.h"

#define REVDEDUP_THREAD_CNT 8

#define REVDEDUP_HASH_TBL_MASK 0x07FFFF
#define REVDEDUP_HASH_TBL_SIZE 524288


typedef struct {
	ImageVersion * prev_iv;
	ImageVersion * this_iv;

	hlist_node * this_segnodes;
	hlist_node * this_blknodes;

	Fingerprint * this_segfps;		// Will use substraction to find its blocknr
	Fingerprint * this_blkfps;		// Will use substraction to find its blocknr

	HashHead seghead[REVDEDUP_HASH_TBL_SIZE];
	HashHead blkhead[REVDEDUP_HASH_TBL_SIZE];
} RevDedup;


typedef struct {
	pthread_mutex_t insert_global_lock;
	pthread_mutex_t search_global_lock;
	pthread_t insert_t[REVDEDUP_THREAD_CNT];
	pthread_t search_t[REVDEDUP_THREAD_CNT];
	pthread_mutex_t insert_lock;
	pthread_mutex_t search_lock;
	uint32_t insert_t_count;
	uint32_t search_t_count;
	pthread_cond_t insert_c;
	pthread_cond_t search_c;
	pthread_cond_t insert_finish_c;
	pthread_cond_t search_finish_c;
	RevDedup * insert_rd;
	RevDedup * search_rd;
	VersionMap * search_map;
	void (*start)();
	void (*stop)();
	void (*buildIndex)(RevDedup * rd, VersionMap * map);
	void (*run)(RevDedup * rd, VersionMap * map);
	RevDedup * (*newRevDedup)(ImageVersion * this_iv, ImageVersion * prev_iv);
	void (*destroyRevDedup)(RevDedup * rd);
} RevDedupService;

RevDedupService * getRevDedupService();

#endif /* REVDEDUP_H_ */
