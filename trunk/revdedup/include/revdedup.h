/*
 * revdedup.h
 *
 *  Created on: 22 Jun, 2013
 *      Author: ngchunho
 */

#ifndef REVDEDUP_H_
#define REVDEDUP_H_

/* common header files */

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

#define MAX_PUNCH(size) (0) //(size * 2 / 10)

#define MAX_ENTRIES 16777216ULL

#define FP_SIZE 20
#define DATA_DIR "data/"

#define AVG_CHUNK_SIZE 8192ULL
#define MIN_CHUNK_SIZE (AVG_CHUNK_SIZE >> 1)
#define MAX_CHUNK_SIZE (AVG_CHUNK_SIZE << 1)

#define AVG_SEG_CHUNKS 128
#define AVG_SEG_SIZE (AVG_SEG_CHUNKS * AVG_CHUNK_SIZE)
#define MIN_SEG_SIZE (AVG_SEG_CHUNKS * MIN_CHUNK_SIZE)
#define MAX_SEG_SIZE (AVG_SEG_CHUNKS * MAX_CHUNK_SIZE)

#define MAX_SEG_CHUNKS (MAX_SEG_SIZE / MIN_CHUNK_SIZE)
#define MAX_COMPRESSED_SIZE (MAX_SEG_SIZE * 17 / 16)

#define COMP_CNT 4

/* 16KB zero block */
#define ZERO_FP "\x89\x72\x56\xb6\x70\x9e\x1a\x4d\xa9\xda\xba\x92\xb6\xbd\xe3\x9c\xcf\xcc\xd8\xc1"
#define ZERO_SIZE MAX_CHUNK_SIZE

#define RAWPTR(x) ((void * )(&x))

#define NODEDUP 0x80000000LU

/* metadata definition */

typedef struct {
	uint8_t fp[FP_SIZE];
	uint32_t ref;
	uint32_t pos;
	uint32_t len;
} CMEntry;

typedef struct {
	uint64_t offset;		// Offset in file
	uint64_t id;			// Chunk ID
	uint32_t pos;			// Position in Bucket
	uint32_t len;			// Length
	uint64_t cid;			// Respective First Chunk ID
	uint16_t chunks;		// Number of Chunks
	uint16_t unique;		// If Segment is unique
	uint8_t fp[FP_SIZE];	// Fingerprint
	uint64_t clen;
	uint8_t * data;			// Data ptr
	uint8_t * cdata;
	CMEntry en[MAX_SEG_CHUNKS];
} Segment;

typedef struct {
	uint8_t fp[FP_SIZE];
	uint32_t ref;
	uint64_t bucket;
	uint32_t pos;
	uint32_t len;
	uint64_t cid;
	uint32_t chunks;
	uint32_t removed;
} SMEntry;

typedef struct {
	uint64_t segID;
} SegmentLog;

typedef struct {
	uint64_t chunkID;
} ChunkLog;

typedef struct {
	uint64_t sid;
	uint32_t segs;
	uint32_t size;
	uint32_t psize;
	uint32_t ver;
	uint32_t inst;
	uint32_t vers;
} BMEntry;

typedef struct {
	uint64_t bucketID;
} BucketLog;

#define INST_MAX 256

typedef struct {
	uint64_t versions;
	uint64_t deleted;
	uint64_t old;
	uint64_t recent;
	struct {
		uint64_t size;
		uint64_t space;
		uint64_t csize;
		uint64_t cspace;
	} vers[255];
} IMEntry;

typedef struct {
	uint64_t pos;
	uint64_t id;
} Direct;

typedef struct {
	uint32_t ptr;		// Entry index in Direct
	uint16_t offset; // Chunk offset in segment
	union {
		uint16_t count;	 // Chunks in segment
		uint16_t version;
	};
} Indirect;

#define BUCKET_SIZE (MAX_SEG_SIZE * 4)
#define BLOCK_SIZE 4096

typedef struct {
	uint64_t id;
	uint64_t sid;
	uint32_t segs;
	uint32_t size;
	int32_t fd;
	int32_t res;
} Bucket;

#define REV_TCNT 8

#define MMAP_FD(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
#define MMAP_FD_RO(fd, size) mmap(0, size, PROT_READ, MAP_SHARED, fd, 0)
#define MMAP_MM(size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
#define MMAP_FD_PV(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

/* Time functions */
#define TIMERSTART(x) gettimeofday(&x, NULL)
#define TIMERSTOP(x) do { struct timeval a = x, b; gettimeofday(&b, NULL); timersub(&b, &a, &x); } while (0)


#endif /* REVDEDUP_H_ */
