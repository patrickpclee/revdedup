/*
 * compression.c
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */


#include "compress.h"
#include "minilzo.h"

static CompressService cps;

static void * process(void * ptr) {
	uint8_t * buf = malloc(LZO1X_MEM_COMPRESS);
	Chunk * ch;
	uint64_t self = (uint64_t) ptr;
	uint64_t clen;
	while ((ch = (Chunk *) Dequeue(cps._mq[self])) != NULL) {
		if (ch->unique) {
			lzo1x_1_compress(ch->data, ch->len, ch->cdata, &clen, buf);
			ch->clen = clen;
		}
		Enqueue(cps._dq[self], ch);
	}
	Enqueue(cps._dq[self], NULL);
	free(buf);
	return NULL;
}

static void * dispatch(void * ptr) {
	Chunk * ch;
	uint64_t turn = 0;
	while ((ch = (Chunk *)Dequeue(cps._iq)) != NULL) {
		Enqueue(cps._mq[turn], ch);
		if (++turn == COMP_CNT) {
			turn = 0;
		}
	}
	for (turn = 0; turn < COMP_CNT; turn++) {
		Enqueue(cps._mq[turn], NULL);
	}
	return NULL;
}

static void * gather(void * ptr) {
	Chunk * ch;
	uint64_t turn = 0;
	while ((ch = (Chunk *) Dequeue(cps._dq[turn])) != NULL) {
		Enqueue(cps._oq, ch);
		if (++turn == COMP_CNT) {
			turn = 0;
		}
	}
	Enqueue(cps._oq, NULL);
	return NULL;
}

static int start(Queue * iq, Queue * oq) {
	int ret, i;
	cps._iq = iq;
	cps._oq = oq;
	for (i = 0; i < COMP_CNT; i++) {
		cps._mq[i] = NewQueue();
		cps._dq[i] = NewQueue();
		ret = pthread_create(cps._cid + i, NULL, process, (void *)(uint64_t)i);
	}
	ret = pthread_create(&cps._tid, NULL, dispatch, NULL);
	ret = pthread_create(&cps._gid, NULL, gather, NULL);
	return ret;
}

static int stop() {
	int ret, i;
	ret = pthread_join(cps._tid, NULL);
	ret = pthread_join(cps._gid, NULL);
	for (i = 0; i < COMP_CNT; i++) {
		pthread_join(cps._cid[i], NULL);
		free(cps._mq[i]);
		free(cps._dq[i]);
	}
	return ret;
}

static CompressService cps = {
		.start = start,
		.stop = stop,
};

CompressService* GetCompressService() {
	return &cps;
}
