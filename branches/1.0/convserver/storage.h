/*
 * storage.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef STORAGE_H_
#define STORAGE_H_

#include <blockqueue.h>
#include "tdserver.h"
#include "fingerprint.h"
#include "segment.h"
#include "image.h"


#define min(a, b) (a > b ? b : a)
#define max(a, b) (a > b ? a : b)
#define SEGSTORE_META_BLOCKS (max(sizeof(SegmentStoreIndex) * SEG_BLOCKS / BLOCK_SIZE, 1))
#define SEGSTORE_META_SIZE (SEGSTORE_META_BLOCKS * BLOCK_SIZE)

#define SEGSTORE_PREFETCH_THREAD 1

#define PUNCH_RATIO 2
#define SEGSTORE_PUNCH_LIMIT (SEG_BLOCKS * PUNCH_RATIO / 10)
#define SEGSTORE_PUNCH_THREAD 8

#define SEGSTORE_PRESERVE_LIMIT 32

typedef struct {
	short ptr;
	short ref;
} SegmentStoreIndex;

typedef struct {
	list_head ps_head;
	Segment * segment;
	int fd;
	uint32_t flags;
	SegmentStoreIndex * si;
} SegmentStore;


typedef struct StorageService {
	uint32_t loc_cnt;
	char ** locs;
	pthread_t segment_prefetch_t[SEGSTORE_PREFETCH_THREAD];
	Queue * segment_prefetch_q;
	void (*start)(uint32_t loc_cnt, char * locs[]);
	void (*stop)();
	int (*createStores)(Fingerprint * segmentfp, uint32_t cnt, Fingerprint * blockfps);
	SegmentStore * (*getSegmentStore)(Segment * seg);
	void (*putSegmentStore)(SegmentStore * ss);
	void (*writeSegment)(SegmentStore * ss, void * data);
	size_t (*pipeSegment)(SegmentStore * ss, int pipefd, uint32_t lblock, uint32_t cnt);
	void (*prefetchSegmentStore)(Segment * seg);
} StorageService;

StorageService * getStorageService();

#endif /* STORAGE_H_ */
