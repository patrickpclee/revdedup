/*
 * segment.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef SEGMENT_H_
#define SEGMENT_H_

#include <linux_list.h>
#include "fingerprint.h"

#define SEG_DATA_READY 0x0001
#define SEG_DATA_PUNCH 0x0002

typedef struct {
	struct hlist_node node;
	Fingerprint fp;
	int32_t refcnt;
	uint32_t size;
} Segment;

Segment * newSegment(Fingerprint * fp, int32_t refcnt, uint32_t size);

#endif /* SEGMENT_H_ */
