/**
 * @file revdedup.h
 * @brief Definitions for RevDedup
 * @author Ng Chun Ho
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

/**
 * 	Max number of entries in the log. Can set to a very large value
 * 	since revdedup use ftruncate() to create space in the log file, and
 * 	most file systems treats it as a sparse file
 */
#define MAX_ENTRIES(x) ((256ULL << 20) * x)

/**
 * 	Max number of images. Can set to larger values.
 */
#define INST_MAX(x) (256 * x)

/**
 * Data directory for revdedup.
 * Segment contents are placed in DATA_DIR/bucket
 * Segment pointers of images are placed in DATA_DIR/image
 * Other logs - ilog (image log), slog (segment log),
 * clog (chunk log), blog (bucket log) and index (fingerprint to ID)
 * are placed directly under DATA_DIR
 */
#define DATA_DIR "data/"
/**
 * Fingerprint size, SHA-1 takes 20 bytes
 */
#define FP_SIZE 20

/**
 * Block size, usually 4096, no need to change
 */
#define BLOCK_SIZE 4096ULL

/**
 * Bucket size. Change according to performance
 */
#define BUCKET_SIZE 16777216

/**
 * Determining the max. bytes that hole punching should be used
 * instead of segment reconstruction in reverse deduplication.
 */
#define MAX_PUNCH(size) (0) //(size * 2 / 10)

// #define WITH_REAL_DATA		/*!< Comment if there is only fingerprint */

#ifdef WITH_REAL_DATA
#define CHUNK_SHIFT 1			/*!< 0 to switch to fixed size chunking */
// #define DISABLE_COMPRESSION	/*!< Comment to enable compression */
#define AVG_CHUNK_SIZE 8192ULL	/*!< Average chunk size */
#define AVG_SEG_BLOCKS 1024		/*!< Segment size = AVG_SEG_BLOCKS * BLOCK_SIZE */
#else
#define CHUNK_SHIFT 0			/*!< 0 to switch to fixed size chunking */
#define DISABLE_COMPRESSION		/*!< Comment to enable compression */
#define AVG_CHUNK_SIZE 4096ULL	/*!< Average chunk size */
#define AVG_SEG_BLOCKS 1024		/*!< Segment size = AVG_SEG_BLOCKS * BLOCK_SIZE */
#endif

#define MIN_CHUNK_SIZE (AVG_CHUNK_SIZE >> CHUNK_SHIFT)
#define MAX_CHUNK_SIZE (AVG_CHUNK_SIZE << CHUNK_SHIFT)
#define ZERO_SIZE MAX_CHUNK_SIZE

#define AVG_SEG_SIZE (AVG_SEG_BLOCKS * BLOCK_SIZE)
#define MIN_SEG_SIZE (AVG_SEG_SIZE >> CHUNK_SHIFT)
#define MAX_SEG_SIZE (AVG_SEG_SIZE << CHUNK_SHIFT)

#define MAX_SEG_CHUNKS (MAX_SEG_SIZE / MIN_CHUNK_SIZE)

#define CPS_CNT 4				/*!< Compression thread count */
#define DPS_CNT 2				/*!< Decompression thread count */
#define REV_CNT 8				/*!< Reverse deduplication thread count */

#define DISABLE_BLOOM 1			/*!< 1 to disable bloom filter in indexing, 0 otherwise */

// #define PREFETCH_WHOLE_BUCKET	/*!< Comment to prefetch segments specifically */

/**
 * ZERO_FP determines the fingerprint of zero chunk
 * RevDedup skips writing the chunk if it is found zero
 * so that backup and restore can be faster
 * To determine the fingerprint of a zero chunk of particular size, run:
 * dd if=/dev/zero bs=<size> count=1 | sha1sum
 */
#if ZERO_SIZE == 4096
#define ZERO_FP "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d"
#elif ZERO_SIZE == 16384
#define ZERO_FP "\x89\x72\x56\xb6\x70\x9e\x1a\x4d\xa9\xda\xba\x92\xb6\xbd\xe3\x9c\xcf\xcc\xd8\xc1"
#endif

#define UNIQ 0x80000000LU		/*!< flag to determine direct pointer in an indirect entry, don't change */

/* metadata definition */

/** Chunk metadata on disk */
typedef struct {
	uint8_t fp[FP_SIZE];		/*!< Fingerprint of the chunk */
	uint32_t ref;				/*!< Reference count */
	uint32_t pos;				/*!< Position */
	uint32_t len;				/*!< Length */
} CMEntry;

/** Segment metadata on disk */
typedef struct {
	uint8_t fp[FP_SIZE];		/*!< Fingerprint of the segment */
	uint32_t ref;				/*!< Reference count */
	uint64_t bucket;			/*!< Bucket ID */
	uint32_t pos;				/*!< Position */
	uint32_t len;				/*!< Length (after compression) */
	uint64_t cid;				/*!< Respective starting chunk ID */
	uint32_t chunks;			/*!< Number of chunks in segment */
	uint16_t compressed;		/*!< Bit indicating if this chunk is unique */
	uint16_t removed;			/*!< Removed size (not used) */
} SMEntry;

/** Bucket metadata on disk */
typedef struct {
	uint64_t sid;				/*!< Starting segment ID */
	uint32_t segs;				/*!< Number of segment */
	uint32_t size;				/*!< Size of bucket */
	uint32_t psize;				/*!< Size to be punched in deletion (not used) */
	uint32_t ver;				/*!< Version of the bucket, -1 for new buckets */
	uint32_t inst;				/*!< Instance of buckets (not used) */
	uint32_t vers;				/*!< For optimization (not used) */
} BMEntry;

/** Image metadata on disk */
typedef struct {
	uint64_t versions;			/*!< Number of versions */
	uint64_t deleted;			/*!< Number of deleted versions */
	uint64_t old;				/*!< Number of versions that is reverse deduped */
	uint64_t recent;			/*!< Number of versions that is considered new */
	struct {
		uint64_t size;			/*!< Size of image */
		uint64_t space;			/*!< Compressed size of image */
		uint64_t csize;			/*!< Physical space taken (after dedupe.) */
		uint64_t cspace;		/*!< Physical space taken after compression (after dedupe.) */
	} vers[255];
} IMEntry;

/** Segment metadata in memory */
typedef struct {
	uint64_t offset;		/*!< Offset in image */
	uint64_t id;			/*!< Global segment ID */
	uint32_t pos;			/*!< Position in Bucket */
	uint32_t len;			/*!< Length of chunk (before compression) */
	uint64_t cid;			/*!< Respective starting chunk ID */
	uint16_t chunks;		/*!< Number of chunks in segment */
	uint8_t unique;			/*!< Bit indicating if this chunk is unique */
	uint8_t compressed;		/*!< Bit indicating if this chunk is compressed */
	uint8_t fp[FP_SIZE];	/*!< Fingerprint of the segment */
	uint64_t clen;			/*!< Length of chunk (after compression) */
	uint8_t * data;			/*!< Pointer to data */
	uint8_t * cdata;		/*!< Pointer to compressed data */
	CMEntry en[MAX_SEG_CHUNKS];		/*!< Respective chunk entries */
} Segment;

/** Bucket metadata in memory */
typedef struct {
	uint64_t id;			/*!< Bucket ID */
	uint64_t sid;			/*!< Starting segment ID */
	uint32_t segs;			/*!< Number of segments */
	uint32_t size;			/*!< Size of bucket */
	int32_t fd;				/*!< File descriptor */
	int32_t res;			/*!< Not used (for padding) */
} Bucket;

/**
 * Segment log metadata. It is placed in the first entry space
 * in the log file. So the first segment ID to be distributed is 1, not 0.
 */
typedef struct {
	uint64_t segID;			/*!< Global counter of segment ID distributed */
} SegmentLog;

/**
 * Chunk log metadata. It is placed in the first entry space
 * in the log file. So the first chunk ID to be distributed is 1, not 0.
 */
typedef struct {
	uint64_t chunkID;		/*!< Global counter of chunk ID distributed */
} ChunkLog;

/**
 * Bucket log metadata. It is placed in the first entry space
 * in the log file. So the first bucket ID to be distributed is 1, not 0.
 */
typedef struct {
	uint64_t bucketID;		/*!< Global counter of bucket ID distributed */
} BucketLog;

/**
 * Direct entry in the image recipe. It directly references the segment by
 * their ID. In restoration, loop through the recipe file and get the segments
 * from the direct entries.
 */
typedef struct {
	uint64_t index;			/*!< Starting byte of the chunk in that image */
	uint64_t id;			/*!< Referenced segment ID */
} Direct;

/**
 * Indirect entry in the image recipe. It only exists when the image got reversely
 * deduplicated. It works by referencing the segment, the chunk offset and the number of
 * chunks of the future version (if restoring version i, then entries are pointing to
 * version i + 1.
 *
 * In restore, the use of len will be replaced to store the version number of future
 * image that contains the referenced segment.
 */
typedef struct {
	uint32_t ptr;			/*!< Entry index in Direct */
	uint16_t pos;			/*!< Chunk offset in segment */
	union {
		uint16_t len;		/*!< Number of chunks */
		uint16_t ver;		/*!< Referenced version (in restore) */
	};
} Indirect;

/** mmap a file */
#define MMAP_FD(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)
/** mmap a file in read-only mode */
#define MMAP_FD_RO(fd, size) mmap(0, size, PROT_READ, MAP_SHARED, fd, 0)
/** allocate a piece of memory (instead of malloc) */
#define MMAP_MM(size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0)
/** mmap a file which subsequent changes would not affect the data in the file */
#define MMAP_FD_PV(fd, size) mmap(0, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, 0)

#define likely(x)    __builtin_expect (!!(x), 1)
#define unlikely(x)  __builtin_expect (!!(x), 0)

#define TIMERSTART(x) gettimeofday(&x, NULL)
#define TIMERSTOP(x) do { struct timeval a = x, b; gettimeofday(&b, NULL); timersub(&b, &a, &x); } while (0)

/** host in network mode */
#define HOST "localhost"
/** port to send metadata in network mode */
#define META_PORT 5402
/** port to send data in network mode */
#define DATA_PORT 5401
/** port to retrieve images in network mode */
#define SEND_PORT 5400

/** thread count for receiving metadata in network mode */
#define META_THREAD_CNT 1ULL
/** thread memory for receiving metadata in network mode */
#define META_THREAD_MEM (128ULL << 20)

/** thread count for receiving data in network mode */
#define DATA_THREAD_CNT 16ULL
/** thread memory for receiving data in network mode */
#define DATA_THREAD_MEM (MAX_SEG_SIZE * 2)

/** thread count for sending images in network mode */
#define SEND_THREAD_CNT 4ULL
/** thread memory for sending images in network mode */
#define SEND_THREAD_MEM (128ULL << 20)


#endif /* REVDEDUP_H_ */

