/*
 * compress.h
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#ifndef COMPRESS_H_
#define COMPRESS_H_

#include <revdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	pthread_t _gid;
	pthread_t _cid[COMP_CNT];
	Queue * _iq;	// In-Queue
	Queue * _oq;	// Out-Queue
	Queue * _mq[COMP_CNT];
	Queue * _dq[COMP_CNT];
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
} CompressService;

CompressService* GetCompressService();

#endif /* COMPRESS_H_ */
