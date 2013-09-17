/*
 * rabin.c
 *
 *  Created on: May 28, 2013
 *      Author: chng
 */

#include "rabin.h"
#include "fingerprint.h"

#define Q(x) (x & (q - 1))			// q-bit mask

static const uint64_t d = 587;				// base
static const uint64_t m = 32;				// window size
static const uint64_t mv = 0x87;
static const uint64_t q = AVG_CHUNK_SIZE;	// average size of chunks

static uint64_t _t[256];		// pre-computed (t_s * d^m) mod q

RabinService service;

static inline Chunk * newChunk(uint64_t offset, uint8_t * data, uint32_t len) {
	Chunk * ch = malloc(sizeof(Chunk));
	ch->offset = offset;
	ch->len = len;
	ch->data = data;
	return ch;
}

void * process(void * ptr) {
	uint8_t * _b = service._data;
	uint64_t v = 0;

	uint64_t b_ptr = 0;
	uint64_t r_ptr = m;

	uint64_t i;
	for (i = 0; i < m; i++) {
		v = Q(d * v + _b[i]);
	}

	v = Q(v);

	while (1) {
		v = Q((d * v) - _t[_b[r_ptr - m]] + _b[r_ptr]);
		r_ptr++;
		if (unlikely(r_ptr == service._size)) {
			Chunk * ch = newChunk(b_ptr, _b + b_ptr, r_ptr - b_ptr);
			b_ptr = r_ptr;
			Enqueue(service._q, ch);
			break;
		}
		if (unlikely(v == mv) || unlikely((r_ptr - b_ptr) == MAX_CHUNK_SIZE)) {
			if (likely((r_ptr - b_ptr) <  MIN_CHUNK_SIZE)) {
				continue;
			}
			Chunk * ch = newChunk(b_ptr, _b + b_ptr, r_ptr - b_ptr);
			b_ptr = r_ptr;
			Enqueue(service._q, ch);
		}
	}
	Enqueue(service._q, NULL);
	return NULL ;
}

int start(uint8_t * data, uint64_t size, Queue * oq) {
	uint64_t _dm = 1;
	uint64_t i;
	for (i = 1; i <= m; i++) {
		_dm = Q(_dm * d);
	}

	for (i = 0; i < 256; i++) {
		_t[i] = Q(i * _dm);
	}

	service._size = size;
	service._data = data;
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
