/*
 * compress.c
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#include "compress.h"
#include "minilzo.h"

static CompressService service;

static void * process(void * ptr) {
	uint8_t buf[LZO1X_MEM_COMPRESS];
	uint8_t * temp = MMAP_MM(MAX_SEG_SIZE);
	uint64_t self = (uint64_t) ptr;
	uint32_t pos = 0, i;
	Segment * seg;
	while ((seg = (Segment *) Dequeue(service._mq[self])) != NULL) {
		if (!seg->unique) {
			Enqueue(service._dq[self], seg);
			continue;
		}
		/* remove zero chunks */
		for (i = 0, pos = 0; i < seg->chunks; i++) {
			if (!memcmp(seg->en[i].fp, ZERO_FP, FP_SIZE)) {
				seg->en[i].pos = pos;
				seg->en[i].len = 0;
			} else {
				memcpy(temp + pos, seg->data + seg->en[i].pos,
						seg->en[i].len);
				seg->en[i].pos = pos;
				pos += seg->en[i].len;
			}
		}

#ifdef DISABLE_COMPRESSION
		memcpy(seg->cdata, temp, pos);
		seg->clen = pos;
		seg->compressed = 0;

#else
		lzo1x_1_compress(temp, pos, seg->cdata, &seg->clen, buf);
		if (seg->clen >= pos) {
			memcpy(seg->cdata, temp, pos);
			seg->clen = pos;
			seg->compressed = 0;
		} else {
			seg->compressed = 1;
		}
#endif
		Enqueue(service._dq[self], seg);
	}
	Enqueue(service._dq[self], NULL);
	munmap(temp, MAX_SEG_SIZE);
	return NULL;
}

static void * dispatch(void * ptr) {
	Segment * seg;
	uint64_t turn = 0;
	while ((seg = (Segment *) Dequeue(service._iq)) != NULL) {
		Enqueue(service._mq[turn], seg);
		if (++turn == CPS_CNT) {
			turn = 0;
		}
	}
	for (turn = 0; turn < CPS_CNT; turn++) {
		Enqueue(service._mq[turn], NULL);
	}
	return NULL;
}

static void * gather(void * ptr) {
	Segment * seg;
	uint64_t turn = 0;
	while ((seg = (Segment *) Dequeue(service._dq[turn])) != NULL) {
		Enqueue(service._oq, seg);
		if (++turn == CPS_CNT) {
			turn = 0;
		}
	}
	Enqueue(service._oq, NULL);
	return NULL;
}

static int start(Queue * iq, Queue * oq) {
	int ret, i;
	service._iq = iq;
	service._oq = oq;
	for (i = 0; i < CPS_CNT; i++) {
		service._mq[i] = NewQueue();
		service._dq[i] = NewQueue();
		ret = pthread_create(service._cid + i, NULL, process,
				(void *) (uint64_t) i);
	}
	ret = pthread_create(&service._tid, NULL, dispatch, NULL);
	ret = pthread_create(&service._gid, NULL, gather, NULL);
	return ret;
}

static int stop() {
	int ret, i;
	ret = pthread_join(service._tid, NULL);
	ret = pthread_join(service._gid, NULL);
	for (i = 0; i < CPS_CNT; i++) {
		pthread_join(service._cid[i], NULL);
		free(service._mq[i]);
		free(service._dq[i]);
	}
	return ret;
}

static CompressService service = {
		.start = start,
		.stop = stop,
};

CompressService* GetCompressService() {
	return &service;
}
