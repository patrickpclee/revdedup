/**
 * @file queue.h
 * @brief Implementation of a blocking queue with pre-defined length
 * @author Ng Chun Ho
 */

#ifndef QUEUE_H_
#define QUEUE_H_

#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <math.h>
#include "revdedup.h"

/** Length of a normal queue */
#define QUEUE_LENGTH ((1024ULL << 20) / MAX_SEG_SIZE)
/** Length of a long queue */
#define LONGQUEUE_LENGTH ((4096ULL << 20) / MAX_SEG_SIZE)
/** Length of a super long queue */
#define SUPERQUEUE_LENGTH ((16384ULL << 20) / MAX_SEG_SIZE)

/**
 * Definition of a queue.
 */
typedef struct {
	volatile uint32_t r_ptr;		/*!< Position for dequeue */
	volatile uint32_t w_ptr;		/*!< Position for enqueue */
	volatile uint32_t cur_len;		/*!< Current length. Block when it equals len */
	uint32_t len;					/*!< Length of the queue */
	pthread_cond_t cond_r;			/*!< Signal when dequeue can continue */
	pthread_cond_t cond_w;			/*!< Signal when enqueue can continue */
	pthread_mutex_t mutex;			/*!< Wait when dequeuing zero queue and enqueueing full queue */
	void * qh[SUPERQUEUE_LENGTH];	/*!< Queue elements */
} Queue;

/**
 * Enqueueing routine
 * @param q Queue for enqueue
 * @param ptr Pointer to data (or data s.t. sizeof(data) <= sizeof(pointer)) to enqueue
 */
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

/**
 * Dequeueing routine
 * @param q	Queue for dequeue
 */
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

/**
 * Create a normal queue
 * @return Queue
 */
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

/**
 * Create a long queue
 * @return Queue
 */
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

/**
 * Create a super long queue
 * @return Queue
 */
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

/**
 * Destroy a queue
 * @param q Queue to destroy
 */
static inline void DelQueue(Queue * q) {
	free(q);
}

#endif /* QUEUE_H_ */
