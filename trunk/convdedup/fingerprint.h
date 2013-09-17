/*
 * fingerprint.h
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#ifndef FINGERPRINT_H_
#define FINGERPRINT_H_

#include <convdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	Queue * _iq;	// In-Queue
	Queue * _oq;	// Out-Queue
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
} FpService;

FpService* GetFpService();

#endif /* FINGERPRINT_H_ */
