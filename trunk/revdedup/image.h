/*
 * image.h
 *
 *  Created on: May 30, 2013
 *      Author: chng
 */

#ifndef IMAGE_H_
#define IMAGE_H_

#include <revdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	Queue * _iq;	// In-Queue
	Queue * _oq;
	IMEntry * _en;
	uint32_t _ins;
	uint32_t _ver;
	uint64_t _cspace;
	int _fd;
	int (*start)(Queue * iq, Queue * oq, uint32_t instanceID);
	int (*stop)();
} ImageService;

ImageService* GetImageService();

#endif /* IMAGE_H_ */
