/*
 * revrbd.h
 *
 *  Created on: 20 Jun, 2013
 *      Author: ngchunho
 */

#ifndef REVRBD_H_
#define REVRBD_H_

#include <revdedup.h>
#include <queue.h>
#include <kclangc.h>

typedef struct {
	pthread_t _tid;
	pthread_t _pbid;
	pthread_t _rsid[REV_CNT];
	pthread_t _ssid;
	pthread_t _pfid;
	pthread_t _rbid;

	uint32_t _ver;

	Queue * _bq;
	Queue * _rsq;
	Queue * _nbq;
	Queue * _pfq;
	Queue * _rbq;

	SMEntry * _sen;
	SegmentLog * _slog;
	CMEntry * _cen;
	ChunkLog * _clog;
	BMEntry * _ben;
	BucketLog * _blog;
	KCDB * __db;
	uint8_t _padding[BLOCK_SIZE];
	int (*start)(SMEntry * sen, CMEntry * cen, BMEntry * ben, uint32_t ver);
	int (*stop)();
} RevRbdService;

RevRbdService * GetRevRbdService();

#endif /* REVRBD_H_ */
