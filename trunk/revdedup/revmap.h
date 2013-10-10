/*
 * revmap.h
 *
 *  Created on: Jun 17, 2013
 *      Author: chng
 */

#ifndef REVMAP_H_
#define REVMAP_H_

#include <revdedup.h>

typedef struct {
	pthread_t _tid[REV_CNT];
	SMEntry * _sen;
	CMEntry * _cen;
	SegmentLog * _slog;
	ChunkLog * _clog;
	uint32_t _ins;
	uint32_t _ver;
	pthread_spinlock_t _rlock;
	int (*start)(SMEntry * sen, CMEntry * cen, uint32_t instances, uint32_t version);
	int (*stop)();
} RevMapService;

RevMapService * GetRevMapService();

#endif /* REVMAP_H_ */
