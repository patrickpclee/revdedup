/*
 * database.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef DATABASE_H_
#define DATABASE_H_

#include <mongo.h>
#include <pthread.h>
#include "rdserver.h"
#include "fingerprint.h"
#include "segment.h"
#include "image.h"

typedef struct {
	struct list_head head;
	mongo conn;
	pthread_mutex_t lock;
} DBResource;

typedef struct {
	DBResource * resource;
	uint32_t conn_ptr;
	pthread_mutex_t global_lock;
	mongo_write_concern wc;
	int (*start)();
	void (*stop)();


	Segment * (*findSegment)(Fingerprint * fp);
	int (*insertSegment)(Segment * seg);
	void (*updateSegment)(Segment * seg);

	Image * (*findImage)(const char * name);
	void (*insertImage)(Image * image);
	void (*updateImage)(Image * image);

	ImageVersion * (*findImageVersion)(Image * image, uint64_t version);
	void (*insertImageVersion)(ImageVersion * imgver);
	void (*updateImageVersion)(ImageVersion * imgver);
} DBService;

DBService * getDBService();

#endif /* DATABASE_H_ */
