/*
 * revdedup.c
 *
 *	Handles reverse deduplication process
 *
 *  Created on: Dec 10, 2012
 *      Author: chng
 */

#include <linux_list.h>
#include <blockqueue.h>
#include "rdserver.h"
#include "database.h"
#include "revdedup.h"
#include "storage.h"
#include "vmrebuilder.h"
#include <stdio.h>

static RevDedupService service;

static void * insertFn(void * cls) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	long tid = (long) (cls);		// Trick, tell the thread their ID
	uint32_t i, j;
	while (1) {
		// Wait for the signal
		pthread_mutex_lock(&service.insert_lock);
		pthread_cond_wait(&service.insert_c, &service.insert_lock);
		pthread_mutex_unlock(&service.insert_lock);

		// Run!
		RevDedup * rd = service.insert_rd;
		if (!rd)
			break;

			
		for (i = tid; i < rd->this_iv->seg_cnt; i += REVDEDUP_THREAD_CNT) {
			uint64_t sp = hash(&rd->this_segfps[i], REVDEDUP_HASH_TBL_MASK);
			pthread_spin_lock(&rd->seghead[sp].lock);
			hlist_add_head(&rd->this_segnodes[i], &rd->seghead[sp].head);
			pthread_spin_unlock(&rd->seghead[sp].lock);

			Segment * seg = dbs->findSegment(&rd->this_segfps[i]);
			BlockFPStore * bs = sts->getBlockFPStore(seg);
			memcpy(rd->this_blkfps + i * SEG_BLOCKS, bs->blockfps,
					bs->count * sizeof(Fingerprint));
			for (j = 0; j < bs->count; j++) {
				if (FP_IS_ZERO(&bs->blockfps[j])) {
					continue;
				}
				uint64_t bp = hash(&rd->this_blkfps[i * SEG_BLOCKS + j],
						REVDEDUP_HASH_TBL_MASK);

				pthread_spin_lock(&rd->blkhead[bp].lock);
				hlist_add_head(&rd->this_blknodes[i * SEG_BLOCKS + j],
						&rd->blkhead[bp].head);
				pthread_spin_unlock(&rd->blkhead[bp].lock);
			}
			sts->putBlockFPStore(bs);

		}
		pthread_mutex_lock(&service.insert_lock);
		service.insert_t_count--;
		if (!service.insert_t_count) {
			pthread_cond_signal(&service.insert_finish_c);
		}
		pthread_mutex_unlock(&service.insert_lock);
	}
	return NULL ;
}

static void * searchFn(void * cls) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	long tid = (long) (cls);		// Trick, tell the thread their ID
	uint32_t i, j;
	while (1) {
		// Wait for the signal
		pthread_mutex_lock(&service.search_lock);
		pthread_cond_wait(&service.search_c, &service.search_lock);
		pthread_mutex_unlock(&service.search_lock);

		// Run!
		RevDedup * rd = service.search_rd;
		VersionMap * map = service.search_map;
		if (!rd)
			break;

		for (i = tid; i < rd->prev_iv->seg_cnt; i += REVDEDUP_THREAD_CNT) {	
			hlist_node * segnode, * blknode;
			uint32_t blocks = SEG_BLOCKS;
			int done = 0;
			if (i == rd->prev_iv->seg_cnt - 1) {
				blocks = (rd->prev_iv->size / BLOCK_SIZE - 1) % SEG_BLOCKS + 1;
			}

			// Segment Deduplication
			uint64_t sp = hash(&rd->prev_iv->segmentfps[i], REVDEDUP_HASH_TBL_MASK);
			hlist_for_each(segnode, &rd->seghead[sp].head) {
				Fingerprint * segfp = &rd->this_segfps[segnode - rd->this_segnodes];
				if (FP_EQUAL(segfp, &rd->prev_iv->segmentfps[i])) {
					uint32_t cand_seg = segfp - rd->this_segfps;
					for (j = 0; j < blocks; j++) {
						map->ptr[i * SEG_BLOCKS + j] = cand_seg * SEG_BLOCKS + j;
					}
					done = 1;
					break;
				}
			}
			if (done) {
				continue;
			}
			// Block Deduplication
			Segment * seg = dbs->findSegment(&rd->prev_iv->segmentfps[i]);
			//sts->prefetchSegmentStore(seg);
			SegmentStore * ss = sts->getSegmentStore(seg);
			BlockFPStore * bs = sts->getBlockFPStore(seg);
			for (j = 0; j < blocks; j++) {
				if (FP_IS_ZERO(&bs->blockfps[j])) {
					map->ptr[i * SEG_BLOCKS + j] = VMAP_ZERO;
					continue;
				}
				uint64_t bp = hash(&bs->blockfps[j], REVDEDUP_HASH_TBL_MASK);
				int32_t cand_blk = VMAP_THIS;


				hlist_for_each(blknode, &rd->blkhead[bp].head) {
					Fingerprint * blkfp = &rd->this_blkfps[blknode - rd->this_blknodes];
					if (FP_EQUAL(blkfp, &bs->blockfps[j])) {
						cand_blk = (blkfp - rd->this_blkfps);
						if (cand_blk == i * SEG_BLOCKS + j) {
							break;
						}
					}
				}
				if (cand_blk != VMAP_THIS) {
					map->ptr[i * SEG_BLOCKS + j] = cand_blk;
				} else {
					map->ptr[i * SEG_BLOCKS + j] = VMAP_THIS;
					ss->si[j].ref++;
				}
			}
			sts->putBlockFPStore(bs);
			if (!seg->refcnt) {
				sts->punchSegmentStore(ss);
			} else {
				sts->putSegmentStore(ss);
			}
		}
		pthread_mutex_lock(&service.search_lock);
		service.search_t_count--;
		if (!service.search_t_count) {
			pthread_cond_signal(&service.search_finish_c);
		}
		pthread_mutex_unlock(&service.search_lock);
	}
	return NULL ;
}

static void buildIndex(RevDedup * rd, VersionMap * map) {
	// About to insert
	pthread_mutex_lock(&service.insert_global_lock);
	service.insert_rd = rd;
	service.insert_t_count = REVDEDUP_THREAD_CNT;
	// Tell the threads to run
	pthread_cond_broadcast(&service.insert_c);

	// Wait for their finish
	pthread_mutex_lock(&service.insert_lock);
	pthread_cond_wait(&service.insert_finish_c, &service.insert_lock);
	pthread_mutex_unlock(&service.insert_lock);

	// All things done
	pthread_mutex_unlock(&service.insert_global_lock);
}

static void run(RevDedup * rd, VersionMap * map) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	uint32_t i;
	// Search
	pthread_mutex_lock(&service.search_global_lock);
	service.search_rd = rd;
	service.search_map = map;
	service.search_t_count = REVDEDUP_THREAD_CNT;
	// Tell the threads to run
	pthread_cond_broadcast(&service.search_c);

	// Wait for their finish
	pthread_mutex_lock(&service.search_lock);
	pthread_cond_wait(&service.search_finish_c, &service.search_lock);
	pthread_mutex_unlock(&service.search_lock);
	
	for (i = 0; i < rd->prev_iv->seg_cnt; i++) {
		Segment * seg = dbs->findSegment(&rd->prev_iv->segmentfps[i]);
		if (seg->refcnt <= 0) {
			sts->purgeBlockFPStore(seg);
		}
	}
	
	pthread_mutex_unlock(&service.search_global_lock);

}

static RevDedup * newRevDedup(ImageVersion * this_iv, ImageVersion * prev_iv) {
	RevDedup * rd = malloc(sizeof(RevDedup));
	uint32_t i;
	rd->prev_iv = prev_iv;
	rd->this_iv = this_iv;

	rd->this_segnodes = malloc(this_iv->seg_cnt * sizeof(hlist_node));
	if (!rd->this_segnodes) {
		printf("cannot allocate segnodes\n");
	}
	rd->this_blknodes = malloc(this_iv->seg_cnt * SEG_BLOCKS * sizeof(hlist_node));

	if (!rd->this_blknodes) {
		printf("cannot allocate blknodes\n");
	}
	rd->this_segfps = this_iv->segmentfps;

	rd->this_blkfps = malloc(this_iv->seg_cnt * SEG_BLOCKS * sizeof(Fingerprint));
	if (!rd->this_blkfps) {
		printf("cannot allocate blkfps\n");
	}
	
	for (i = 0; i < REVDEDUP_HASH_TBL_SIZE; i++) {
		INIT_HLIST_HEAD(&rd->seghead[i].head);
		pthread_spin_init(&rd->seghead[i].lock, PTHREAD_PROCESS_PRIVATE);
		INIT_HLIST_HEAD(&rd->blkhead[i].head);
		pthread_spin_init(&rd->blkhead[i].lock, PTHREAD_PROCESS_PRIVATE);
	}
	
	return rd;
}

static void destroyRevDedup(RevDedup * rd) {
	free(rd->this_segnodes);
	free(rd->this_blknodes);
	free(rd->this_blkfps);
	free(rd);
}

static void start() {
	long i;
	pthread_mutex_init(&service.insert_global_lock, NULL );
	pthread_mutex_init(&service.search_global_lock, NULL );
	pthread_mutex_init(&service.insert_lock, NULL );
	pthread_mutex_init(&service.search_lock, NULL );
	pthread_cond_init(&service.insert_c, NULL );
	pthread_cond_init(&service.search_c, NULL );
	pthread_cond_init(&service.insert_finish_c, NULL );
	pthread_cond_init(&service.search_finish_c, NULL );
	for (i = 0; i < REVDEDUP_THREAD_CNT; i++) {
		pthread_create(&service.insert_t[i], NULL, insertFn, (void *) i);// Trick, tell the thread their ID
		pthread_create(&service.search_t[i], NULL, searchFn, (void *) i);// Trick, tell the thread their ID
	}
}

static void stop() {
	int i;
	for (i = 0; i < REVDEDUP_THREAD_CNT; i++) {
		service.insert_rd = NULL;
		service.search_rd = NULL;
		pthread_cond_broadcast(&service.insert_c);
		pthread_cond_broadcast(&service.search_c);
		pthread_join(service.insert_t[i], NULL );
		pthread_join(service.search_t[i], NULL );
	}
}

static RevDedupService service = {
		.start = start,
		.stop = stop,
		.buildIndex = buildIndex,
		.run = run,
		.newRevDedup = newRevDedup,
		.destroyRevDedup = destroyRevDedup,
};

RevDedupService * getRevDedupService() {
	return &service;
}

