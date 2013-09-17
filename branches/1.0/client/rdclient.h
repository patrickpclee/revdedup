/*
 * tdclient.h
 *
 *  Created on: Dec 7, 2012
 *      Author: chng
 */

#ifndef TDCLIENT_H_
#define TDCLIENT_H_

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#define HOST "127.0.0.1"
#define PORT 5749

#define BLOCK_SIZE 4096
//#define SEG_BLOCKS 1024
//#define SEG_BLOCKS 2048
//#define SEG_BLOCKS 4096
#define SEG_BLOCKS 8192
#define SEG_SIZE (BLOCK_SIZE * SEG_BLOCKS)

#define FP_TYPE MHASH_SHA1
#define FP_SIZE 20

#define STRING_BUF_SIZE 128
#define HTTP_HEADER_MAX 16

#define FPCOMP_THREAD_CNT 8
#define PUTSEG_THREAD_CNT 16

typedef struct {
	uint64_t id;
	uint32_t blocks;
	uint32_t exist;
	char segmentfp[STRING_BUF_SIZE];
	uint8_t blockfp[SEG_BLOCKS][FP_SIZE];
	uint8_t * data;
} Segment;

typedef struct {
	const char * name;
	size_t size;
	uint64_t blocks;
	int fd;
	uint32_t segcnt;
	Segment * segments;
} Image;

#include <blockqueue.h>

typedef struct {
	Queue * in;
	Queue * out;
	pthread_t * threads;
	void (*segmentExist)(const char * name, uint32_t seg_cnt, Segment * segs);
	void (*putImage)(Image * image);
	void (*start) (Queue * in, Queue * out);
	void (*stop)();
} SegmentService;

SegmentService * getSegmentService() ;

#endif /* TDCLIENT_H_ */
