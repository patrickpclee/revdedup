/*
 * index.h
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#ifndef INDEX_H_
#define INDEX_H_

#include <revdedup.h>
#include <queue.h>
#include <kclangc.h>
#include "bloom.h"

typedef struct {
	pthread_t _tid;
	Queue * _iq;	// In-Queue
	Queue * _oq;	// Out-Queue
	SegmentLog * _slog;
	SMEntry * _sen;
	ChunkLog * _clog;
	CMEntry * _cen;
	KCDB * _db;
	Bloom _bl;
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
	int (*setSegment)(Segment * seg, uint64_t bucket);
} IndexService;

IndexService* GetIndexService();

#endif /* INDEX_H_ */
