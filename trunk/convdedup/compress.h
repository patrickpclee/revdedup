/*
 * compression.h
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#ifndef COMPRESSION_H_
#define COMPRESSION_H_

#include <convdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	pthread_t _gid;
	pthread_t _cid[COMP_CNT];
	Queue * _iq;	// In-Queue
	Queue * _mq[COMP_CNT];
	Queue * _dq[COMP_CNT];
	Queue * _oq;	// Out-Queue
	uint64_t _turn;
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
} CompressService;

CompressService* GetCompressService();

#endif /* COMPRESSION_H_ */
