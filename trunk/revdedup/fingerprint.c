/**
 * @file	fingerprint.c
 * @brief	Fingerprint Service Implementation
 * @author	Ng Chun Ho
 */

#include <openssl/sha.h>
#include "fingerprint.h"

static FpService service;

/** Define cryptographic function used */
#define FP_COMPUTE(in, size, out) SHA1(in, size, out)

/**
 * Main loop for processing segments
 * @param ptr		useless
 */
static void * process(void * ptr) {
	while (1) {
		Segment * seg = (Segment *)Dequeue(service._iq);
		if (seg == NULL) {
			break;
		}
		int i;
		for (i = 0; i < seg->chunks; i++) {
			FP_COMPUTE(seg->data + seg->en[i].pos, seg->en[i].len, seg->en[i].fp);
		}
		FP_COMPUTE((uint8_t *)seg->en, seg->chunks * sizeof(CMEntry), seg->fp);
		Enqueue(service._oq, seg);
	}
	Enqueue(service._oq, NULL);
	return NULL;
}

/**
 * Implements FpService->start()
 */
static int start(Queue * iq, Queue * oq) {
	service._iq = iq;
	service._oq = oq;
	int ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

/**
 * Implements FpService->stop()
 */
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
