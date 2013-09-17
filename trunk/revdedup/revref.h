/*
 * revsegref.h
 *
 *  Created on: Jun 14, 2013
 *      Author: chng
 */

#ifndef REVREF_H_
#define REVREF_H_

#include <revdedup.h>

typedef struct {
	pthread_t _tid;
	SMEntry * _sen;
	SegmentLog * _slog;
	uint32_t _ins;
	uint32_t _ver;
	int (*start)(SMEntry * sen, uint32_t instances, uint32_t version);
	int (*stop)();
} RevRefService;

RevRefService * GetRevRefService();

#endif /* REVSEGREF_H_ */
