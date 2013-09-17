/*
 * image.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef IMAGE_H_
#define IMAGE_H_

#include <pthread.h>
#include "tdserver.h"
#include "fingerprint.h"


typedef struct {
	hlist_node node;		// for the list of Images
	char name[STRING_BUF_SIZE];
	uint64_t versions;
	hlist_head head;		// list of ImageVersions
	pthread_spinlock_t lock;
} Image;

typedef struct {
	hlist_node node;
	uint64_t version;
	uint64_t size;
	uint32_t seg_cnt;
	Image * img;
	Fingerprint * segmentfps;
} ImageVersion;

Image * newImage(const char * name);
ImageVersion * newImageVersion(Image * image, uint64_t size, uint32_t seg_cnt,
		Fingerprint * segmentfps);

#endif /* IMAGE_H_ */
