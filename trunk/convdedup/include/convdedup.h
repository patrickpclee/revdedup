/*
 * convdedup.h
 *
 *  Created on: Jul 15, 2013
 *      Author: chng
 */

#ifndef CONVDEDUP_H_
#define CONVDEDUP_H_


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

#define MAX_ENTRIES 16777216ULL

#define FP_SIZE 20
#define DATA_DIR "data/"

#define AVG_CHUNK_SIZE 8192ULL
#define MIN_CHUNK_SIZE (AVG_CHUNK_SIZE >> 1)
#define MAX_CHUNK_SIZE (AVG_CHUNK_SIZE << 1)

#define MAX_COMPRESSED_SIZE (MAX_CHUNK_SIZE * 17 / 16)

#define COMP_CNT 4

#define RAWPTR(x) ((void * )(&x))

/* metadata definition */

typedef struct {
	uint64_t offset;		// Offset in file
	uint64_t id;			// Chunk ID
	uint32_t pos;			// Position in Bucket
	uint32_t len;			// Length
	uint8_t fp[FP_SIZE];	// Fingerprint
	/* Live Generated Data */
	uint32_t clen : 31;
	uint32_t unique : 1;		// If Chunk is unique
	uint8_t * data;
	uint8_t * cdata;
} Chunk;

typedef struct {
	uint8_t fp[FP_SIZE];
	uint32_t ref;
	uint64_t bucket;
	uint32_t pos;
	uint32_t len;
} CMEntry;

typedef struct {
	uint64_t chunkID;
} ChunkLog;

typedef struct {
	uint64_t cid;
	uint32_t chunks;
	uint32_t size;
	uint32_t psize;
	uint32_t rsize;
} BMEntry;

typedef struct {
	uint64_t bucketID;
} BucketLog;

typedef struct {
	uint64_t size;
	uint64_t csize;
	uint64_t space;
	uint64_t cspace;
} IMEntry;

typedef struct {
	uint64_t imageID;
} ImageLog;

typedef struct {
	uint64_t index;
	uint64_t id;
} Direct;

#define BUCKET_SIZE 4718592
#define BLOCK_SIZE 4096

typedef struct {
	uint64_t id;
	uint64_t cid;
	uint32_t chunks;
	uint32_t size;
	int32_t fd;
	int32_t res;
} Bucket;

#define MMAP_FD(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
#define MMAP_FD_RO(fd, size) mmap(0, size, PROT_READ, MAP_SHARED, fd, 0)
#define MMAP_MM(size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
#define MMAP_FD_PV(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

/* Time functions */
#define TIMERSTART(x) gettimeofday(&x, NULL)
#define TIMERSTOP(x) do { struct timeval a = x, b; gettimeofday(&b, NULL); timersub(&b, &a, &x); } while (0)


#endif /* CONVDEDUP_H_ */
