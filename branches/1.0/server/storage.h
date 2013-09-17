/*
 * storage.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef STORAGE_H_
#define STORAGE_H_

#include <blockqueue.h>
#include "rdserver.h"
#include "fingerprint.h"
#include "segment.h"
#include "image.h"


#define min(a, b) (a > b ? b : a)
#define max(a, b) (a > b ? a : b)
#define SEGSTORE_META_BLOCKS (max(sizeof(SegmentStoreIndex) * SEG_BLOCKS / BLOCK_SIZE, 1))
#define SEGSTORE_META_SIZE (SEGSTORE_META_BLOCKS * BLOCK_SIZE)

#define SEGSTORE_PREFETCH_THREAD 6
#define BLKSTORE_PREFETCH_THREAD 1

#define PUNCH_RATIO 6
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

typedef struct {
	Segment * segment;
	int fd;
	uint32_t count;
	Fingerprint * blockfps;
} BlockFPStore;

typedef struct {
	ImageVersion * iv;
	int fd;
	uint32_t blocks;
	uint32_t * ptr;
} VersionMap;

typedef struct StorageService {
	uint32_t loc_cnt;
	char ** locs;
	pthread_t segment_prefetch_t[SEGSTORE_PREFETCH_THREAD];
	Queue * segment_prefetch_q;
	pthread_t blockfp_prefetch_t[BLKSTORE_PREFETCH_THREAD];
	Queue * blockfp_prefetch_q;
	pthread_t segment_punch_t[SEGSTORE_PUNCH_THREAD];
	Queue * segment_punch_q;
	void (*start)(uint32_t loc_cnt, char * locs[]);
	void (*stop)();
	int (*createStores)(Fingerprint * segmentfp, uint32_t cnt, Fingerprint * blockfps);
	SegmentStore * (*getSegmentStore)(Segment * seg);
	void (*putSegmentStore)(SegmentStore * ss);
	void (*writeSegment)(SegmentStore * ss, BlockFPStore * bs, void * data);
	size_t (*pipeSegment)(SegmentStore * ss, int pipefd, uint32_t lblock, uint32_t cnt);
	size_t (*pipeZero)(int pipefd);
	BlockFPStore * (*getBlockFPStore)(Segment * seg);
	void (*putBlockFPStore)(BlockFPStore * bs);
	void (*purgeBlockFPStore)(Segment * seg);
	VersionMap * (*createVersionMap)(ImageVersion * iv);
	VersionMap * (*getVersionMap)(ImageVersion * iv);
	void (*putVersionMap)(VersionMap * map);
	void (*prefetchSegmentStore)(Segment * seg);
	void (*prefetchBlockFPStore)(Segment * seg);
	void (*punchSegmentStore)(SegmentStore * ss);
} StorageService;

StorageService * getStorageService();

#endif /* STORAGE_H_ */
