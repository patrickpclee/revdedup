/*
 * rabin.h
 *
 *  Created on: May 28, 2013
 *      Author: chng
 */

#ifndef RABIN_H_
#define RABIN_H_

#include <convdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	Queue * _q;
	uint8_t * _data;
	uint64_t _size;
	int (*start)(uint8_t * data, uint64_t size, Queue * q);
	int (*stop)();
} RabinService;

RabinService* GetRabinService();

#endif /* RABIN_H_ */
