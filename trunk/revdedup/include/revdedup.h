/*
 * revdedup.h
 *
 *  Created on: 22 Jun, 2013
 *      Author: ngchunho
 */

#ifndef REVDEDUP_H_
#define REVDEDUP_H_

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <endian.h>
#include <assert.h>

/* global parameter definition */

#define MAX_ENTRIES(x) ((256ULL << 20) * x)
#define DATA_DIR "data/"
#define FP_SIZE 20
#define BLOCK_SIZE 4096ULL
#define BUCKET_SIZE 16777216
#define MAX_PUNCH(size) (0) //(size * 2 / 10)

// #define WITH_REAL_DATA		/** Comment if there is only fingerprint */

#ifdef WITH_REAL_DATA
#define CHUNK_SHIFT 1		/** 0 to switch to fixed size chunking */
// #define DISABLE_COMPRESSION	/** Comment to enable compression */
#define AVG_CHUNK_SIZE 8192ULL
#define AVG_SEG_BLOCKS 1024
#else
#define CHUNK_SHIFT 0		/** 0 to switch to fixed size chunking */
#define DISABLE_COMPRESSION	/** Comment to enable compression */
#define AVG_CHUNK_SIZE 4096ULL
#define AVG_SEG_BLOCKS 1024
#endif


#define MIN_CHUNK_SIZE (AVG_CHUNK_SIZE >> CHUNK_SHIFT)
#define MAX_CHUNK_SIZE (AVG_CHUNK_SIZE << CHUNK_SHIFT)
#define ZERO_SIZE MAX_CHUNK_SIZE

#define AVG_SEG_SIZE (AVG_SEG_BLOCKS * BLOCK_SIZE)
#define MIN_SEG_SIZE (AVG_SEG_SIZE >> CHUNK_SHIFT)
#define MAX_SEG_SIZE (AVG_SEG_SIZE << CHUNK_SHIFT)

#define MAX_SEG_CHUNKS (MAX_SEG_SIZE / MIN_CHUNK_SIZE)

#define CPS_CNT 4
#define DPS_CNT 2
#define REV_CNT 8

#define DISABLE_BLOOM 1

/* 16KB zero block */
#if ZERO_SIZE == 4096
#define ZERO_FP "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d"
#elif ZERO_SIZE == 16384
#define ZERO_FP "\x89\x72\x56\xb6\x70\x9e\x1a\x4d\xa9\xda\xba\x92\xb6\xbd\xe3\x9c\xcf\xcc\xd8\xc1"
#endif

#define UNIQ 0x80000000LU

/* metadata definition */

typedef struct {
	uint8_t fp[FP_SIZE];		/** Fingerprint of the chunk */
	uint32_t ref;				/** Reference count */
	uint32_t pos;				/** Position */
	uint32_t len;				/** Length */
} CMEntry;

typedef struct {
	uint8_t fp[FP_SIZE];		/** Fingerprint of the segment */
	uint32_t ref;				/** Reference count */
	uint64_t bucket;			/** Bucket ID */
	uint32_t pos;				/** Position */
	uint32_t len;				/** Length (after compression) */
	uint64_t cid;				/** Respective starting chunk ID */
	uint32_t chunks;			/** Number of chunks in segment */
	uint16_t compressed;		/** Bit indicating if this chunk is unique */
	uint16_t removed;			/** Removed size (not used) */
} SMEntry;

typedef struct {
	uint64_t sid;				/** Starting segment ID */
	uint32_t segs;				/** Number of segment */
	uint32_t size;				/** Size of bucket */
	uint32_t psize;				/** Size to be punched in deletion (not used) */
	uint32_t ver;				/** Version of the bucket, -1 for new buckets */
	uint32_t inst;				/** Instance of buckets (not used) */
	uint32_t vers;				/** For optimization (not used) */
} BMEntry;

typedef struct {
	uint64_t versions;			/** Number of versions */
	uint64_t deleted;			/** Number of deleted versions */
	uint64_t old;				/** Number of versions that is reverse deduped */
	uint64_t recent;			/** Number of versions that is considered new */
	struct {
		uint64_t size;			/** Size of image */
		uint64_t space;			/** Compressed size of image */
		uint64_t csize;			/** Physical space taken (after dedupe.) */
		uint64_t cspace;		/** Physical space taken after compression (after dedupe.) */
	} vers[255];
} IMEntry;

typedef struct {
	uint64_t offset;		/** Offset in image */
	uint64_t id;			/** Global segment ID */
	uint32_t pos;			/** Position in Bucket */
	uint32_t len;			/** Length of chunk (before compression) */
	uint64_t cid;			/** Respective starting chunk ID */
	uint16_t chunks;		/** Number of chunks in segment */
	uint8_t unique;			/** Bit indicating if this chunk is unique */
	uint8_t compressed;		/** Bit indicating if this chunk is compressed */
	uint8_t fp[FP_SIZE];	/** Fingerprint of the segment */
	uint64_t clen;			/** Length of chunk (after compression) */
	uint8_t * data;			/** Pointer to data */
	uint8_t * cdata;		/** Pointer to compressed data */
	CMEntry en[MAX_SEG_CHUNKS];		/** Respective chunk entries */
} Segment;

typedef struct {
	uint64_t id;			/** Bucket ID */
	uint64_t sid;			/** Starting segment ID */
	uint32_t segs;			/** Number of segments */
	uint32_t size;			/** Size of bucket */
	int32_t fd;				/** File descriptor */
	int32_t res;			/** Not used (for padding) */
} Bucket;

typedef struct {
	uint64_t segID;			/** Global counter of segment ID distributed */
} SegmentLog;

typedef struct {
	uint64_t chunkID;		/** Global counter of chunk ID distributed */
} ChunkLog;

typedef struct {
	uint64_t bucketID;		/** Global counter of bucket ID distributed */
} BucketLog;

#define INST_MAX(x) (256 * x)

typedef struct {
	uint64_t index;			/** Starting byte of the chunk in that image */
	uint64_t id;			/** Referenced chunk ID */
} Direct;

typedef struct {
	uint32_t ptr;			/** Entry index in Direct */
	uint16_t pos;			/** Chunk offset in segment */
	union {
		uint16_t len;		/** Number of chunks */
		uint16_t ver;		/** Referenced version (in restore) */
	};
} Indirect;

/**
 * helper definitions for managable code
 */
#define MMAP_FD(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
#define MMAP_FD_RO(fd, size) mmap(0, size, PROT_READ, MAP_SHARED, fd, 0)
#define MMAP_MM(size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
#define MMAP_FD_PV(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

#define TIMERSTART(x) gettimeofday(&x, NULL)
#define TIMERSTOP(x) do { struct timeval a = x, b; gettimeofday(&b, NULL); timersub(&b, &a, &x); } while (0)

/**
 * Definitions for network transfer
 */
#define HOST "localhost"
#define META_PORT 5402
#define DATA_PORT 5401
#define SEND_PORT 5400

#define META_THREAD_CNT 1ULL
#define META_THREAD_MEM (128ULL << 20)

#define DATA_THREAD_CNT 16ULL
#define DATA_THREAD_MEM (MAX_SEG_SIZE * 2)

#define SEND_THREAD_CNT 4ULL
#define SEND_THREAD_MEM (128ULL << 20)


#endif /* REVDEDUP_H_ */

