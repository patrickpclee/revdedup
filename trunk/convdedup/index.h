/*
 * index.h
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#ifndef INDEX_H_
#define INDEX_H_

#include <convdedup.h>
#include <queue.h>
#include <kclangc.h>
#include "bloom.h"


typedef struct {
	pthread_t _tid;
	Queue * _iq;	// In-Queue
	Queue * _oq;	// Out-Queue
	ChunkLog * _log;
	CMEntry * _en;
	KCDB * _db;
	Bloom _bl;
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
	int (*setChunk)(Chunk * ch, uint64_t bid);
} IndexService;

IndexService* GetIndexService();

#endif /* INDEX_H_ */
