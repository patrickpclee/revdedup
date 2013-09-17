/*
 * rabin.c
 *
 *  Created on: May 28, 2013
 *      Author: chng
 */

#include "rabin.h"
#include "fingerprint.h"

#define LQ(x) (x & (lq - 1))			// q-bit mask
#define UQ(x) (x & (uq - 1))		// uq-bit mask

static const uint64_t d = 587;				// base
static const uint64_t m = 32;				// window size
static const uint64_t mv = 0x87;
static const uint64_t lq = AVG_CHUNK_SIZE;	// average size of chunks
static const uint64_t uq = AVG_SEG_SIZE;	// average size of segments

static uint64_t _t[256];		// pre-computed (t_s * d^m) mod q

RabinService service;

static inline Segment * newSegment(uint64_t offset, uint8_t * ptr) {
	Segment * seg = malloc(sizeof(Segment));
	memset(seg, 0, sizeof(Segment));
	seg->offset = offset;
	seg->len = 0;
	seg->chunks = 0;
	seg->data = ptr;
	return seg;
}

static inline void insChunk(Segment * seg, uint32_t pos, uint32_t len) {
	seg->en[seg->chunks].pos = pos;
	seg->en[seg->chunks].len = len;
	seg->en[seg->chunks].ref = 0;
	seg->len += len;
	seg->chunks++;

}

void * process(void * ptr) {
	uint8_t * _b = service._data;
	uint64_t v = 0;

	uint64_t index = 0;
	uint64_t b_ptr = 0;
	uint64_t r_ptr = m;

	uint64_t i;
	for (i = 0; i < m; i++) {
		v = UQ(d * v + _b[i]);
	}

	Segment * seg = newSegment(index, _b + index);
	while (1) {
		v = UQ((d * v) - _t[_b[r_ptr - m]] + _b[r_ptr]);
		r_ptr++;
		if (unlikely(r_ptr == service._size)) {
			insChunk(seg, b_ptr - index, r_ptr - b_ptr);
			b_ptr = r_ptr;
			Enqueue(service._q, seg);
			break;
		}
		if (seg->len >= MIN_SEG_SIZE) {
			if (v == mv || unlikely(seg->len + r_ptr - b_ptr == MAX_SEG_SIZE)) {
				insChunk(seg, b_ptr - index, r_ptr - b_ptr);
				b_ptr = r_ptr;
				Enqueue(service._q, seg);
				index = b_ptr;
				seg = newSegment(index, _b + index);
				continue;
			}
		}

		if (unlikely(LQ(v) == mv) || unlikely((r_ptr - b_ptr) == MAX_CHUNK_SIZE)) {
			if ((r_ptr - b_ptr) < MIN_CHUNK_SIZE) {
				continue;
			}
			insChunk(seg, b_ptr - index, r_ptr - b_ptr);
			b_ptr = r_ptr;
		}
	}
	Enqueue(service._q, NULL );
	return NULL ;
}

int start(uint8_t * data, uint64_t size, Queue * oq) {
	uint64_t _dm = 1;
	uint64_t i;
	for (i = 1; i <= m; i++) {
		_dm = UQ(_dm * d);
	}

	for (i = 0; i < 256; i++) {
		_t[i] = UQ(i * _dm);
	}

	service._data = data;
	service._size = size;
	service._q = oq;
	int ret = pthread_create(&service._tid, NULL, process, NULL );
	return ret;
}

int stop() {
	int ret = pthread_join(service._tid, NULL);
	return ret;
}

RabinService service = {
		.start = start,
		.stop = stop,
};


RabinService* GetRabinService() {
	return &service;
}
