/*
 * segment.c
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#include <linux_list.h>
#include <string.h>
#include "segment.h"

Segment * newSegment(Fingerprint * fp, int32_t refcnt, uint32_t size) {
	Segment * segment = malloc(sizeof(Segment));
	memcpy(&segment->fp, fp, FP_SIZE);
	segment->refcnt = refcnt;
	segment->size = size;
	return segment;
}

