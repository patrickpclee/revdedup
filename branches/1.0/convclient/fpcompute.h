/*
 * fpcompute.h
 *
 *  Created on: Dec 7, 2012
 *      Author: chng
 */

#ifndef FPCOMPUTE_H_
#define FPCOMPUTE_H_

#include <pthread.h>
#include "tdclient.h"
#include <blockqueue.h>

void printhex(unsigned char * fp);
void bin2hex(unsigned char * fp, char * out);

typedef struct {
	Queue * in;
	Queue * out;
	pthread_t * threads;
	void (*start)(Queue * in, Queue * out);
	void (*stop)();
} FPCompService;

FPCompService * getFPCompService ();

#endif /* FPCOMPUTE_H_ */
