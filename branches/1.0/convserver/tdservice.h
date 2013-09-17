/*
 * tdservice.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef TDSERVICE_H_
#define TDSERVICE_H_

#include <stdio.h>
#include <sys/time.h>
#include "fingerprint.h"

typedef struct {
	void (*start)();
	void (*stop)();
	int (*segmentExist)(uint32_t cnt, Fingerprint * fps, char * exist);
	int (*putSegmentFP)(Fingerprint * segmentfp, uint32_t cnt,
			Fingerprint * blockfps);
	int (*putSegmentData)(Fingerprint * segmentfp, uint32_t size, void * data);
	int (*putImage)(const char * name, uint64_t size, uint32_t cnt,
			Fingerprint * segmentfps);

	int (*getImage)(const char * name, int fd, uint64_t * size);
	int (*getImageVersion)(const char * name, int fd, uint64_t version,
			uint64_t * size);
} TDService;

TDService * getTDService();

#endif /* TDSERVICE_H_ */
