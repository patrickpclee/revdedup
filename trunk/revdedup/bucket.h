/*
 * bucket.h
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#ifndef BUCKET_H_
#define BUCKET_H_

#include <revdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	Queue * _iq;	// In-Queue
	Queue * _oq;	// Out-Queue
	BMEntry * _en;
	BucketLog * _log;
	uint32_t _inst;
	uint32_t _ver;
	uint8_t _padding[BLOCK_SIZE];
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
} BucketService;

BucketService* GetBucketService();

#endif /* BUCKET_H_ */
