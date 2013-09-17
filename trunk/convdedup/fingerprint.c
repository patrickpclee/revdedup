/*
 * fingerprint.c
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#include <openssl/sha.h>
#include "fingerprint.h"

static FpService service;

#define FP_COMPUTE(in, size, out) SHA1(in, size, out)

static void * process(void * ptr) {
	while (1) {
		Chunk * ch = (Chunk *)Dequeue(service._iq);
		if (ch == NULL) {
			break;
		}
		FP_COMPUTE(ch->data, ch->len, ch->fp);
		Enqueue(service._oq, ch);
	}
	Enqueue(service._oq, NULL);
	return NULL;
}

static int start(Queue * iq, Queue * oq) {
	service._iq = iq;
	service._oq = oq;
	int ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

static int stop() {
	int ret = pthread_join(service._tid, NULL);
	return ret;
}

static FpService service = {
		.start = start,
		.stop = stop,
};

FpService* GetFpService() {
	return &service;
}
