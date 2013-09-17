/*
 * fingerprint.h
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#ifndef FINGERPRINT_H_
#define FINGERPRINT_H_

#include "tdserver.h"

typedef struct {
	uint8_t x[FP_SIZE];
} Fingerprint;

static inline uint64_t hash(Fingerprint * fp, uint64_t mask) {
	return (*(uint64_t *)fp) & mask;
}

#define FP_EQUAL(fp1, fp2) (!memcmp((fp1)->x, (fp2)->x, FP_SIZE))

void printhex(Fingerprint * fp);
void bin2hex(Fingerprint * fp, char * out);
void hex2bin(const char * in, Fingerprint * fp);

#endif /* FINGERPRINT_H_ */
