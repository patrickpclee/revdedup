/*
 * queue.h
 *
 *  Created on: May 28, 2013
 *      Author: chng
 */

#ifndef QUEUE_H_
#define QUEUE_H_

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

#define QUEUE_LENGTH 128
#define LONGQUEUE_LENGTH 512
#define SUPERQUEUE_LENGTH 65536

typedef struct {
	volatile uint32_t r_ptr;
	volatile uint32_t w_ptr;
	volatile uint32_t cur_len;
	uint32_t len;
	pthread_cond_t cond_r;
	pthread_cond_t cond_w;
	pthread_mutex_t mutex;
	void * qh[SUPERQUEUE_LENGTH];
} Queue;

static inline void Enqueue(Queue * q, void * ptr) {
	pthread_mutex_lock(&q->mutex);
	while (q->cur_len == q->len) {
		pthread_cond_wait(&q->cond_w, &q->mutex);
	}
	q->cur_len++;
	q->qh[q->w_ptr++] = ptr;
	if (q->w_ptr == q->len) {
		q->w_ptr = 0;
	}
	pthread_cond_signal(&q->cond_r);
	pthread_mutex_unlock(&q->mutex);

}

static inline void * Dequeue(Queue * q) {
	void * ptr;
	pthread_mutex_lock(&q->mutex);
	while (q->cur_len == 0) {
		pthread_cond_wait(&q->cond_r, &q->mutex);
	}
	q->cur_len--;
	ptr = q->qh[q->r_ptr++];
	if (q->r_ptr == q->len) {
		q->r_ptr = 0;
	}
	pthread_cond_signal(&q->cond_w);
	pthread_mutex_unlock(&q->mutex);

	return ptr;
}

static inline Queue * NewQueue() {
	Queue * q = malloc(sizeof(Queue));
	int i;
	q->r_ptr = q->w_ptr = 0;
	q->cur_len = 0;
	q->len = QUEUE_LENGTH;
	pthread_mutex_init(&q->mutex, NULL);
	pthread_cond_init(&q->cond_r, NULL);
	pthread_cond_init(&q->cond_w, NULL);
	return q;
}

static inline Queue * LongQueue() {
	Queue * q = malloc(sizeof(Queue));
	int i;
	q->r_ptr = q->w_ptr = 0;
	q->cur_len = 0;
	q->len = LONGQUEUE_LENGTH;
	pthread_mutex_init(&q->mutex, NULL);
	pthread_cond_init(&q->cond_r, NULL);
	pthread_cond_init(&q->cond_w, NULL);
	return q;
}

static inline Queue * SuperQueue() {
	Queue * q = malloc(sizeof(Queue));
	int i;
	q->r_ptr = q->w_ptr = 0;
	q->cur_len = 0;
	q->len = SUPERQUEUE_LENGTH;
	pthread_mutex_init(&q->mutex, NULL);
	pthread_cond_init(&q->cond_r, NULL);
	pthread_cond_init(&q->cond_w, NULL);
	return q;
}

#endif /* QUEUE_H_ */
