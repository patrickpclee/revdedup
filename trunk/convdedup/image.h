/*
 * file.h
 *
 *  Created on: May 30, 2013
 *      Author: chng
 */

#ifndef FILE_H_
#define FILE_H_

#include <convdedup.h>
#include <queue.h>

typedef struct {
	pthread_t _tid;
	Queue * _iq;	// In-Queue
	Queue * _oq;
	ImageLog * _log;
	IMEntry * _en;
	uint64_t _id;
	uint64_t _size;
	uint64_t _csize;
	uint64_t _space;
	uint64_t _cspace;
	int _fd;
	int (*start)(Queue * iq, Queue * oq);
	int (*stop)();
} ImageService;

ImageService* GetImageService();

#endif /* FILE_H_ */
