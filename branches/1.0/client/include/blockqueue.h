/*
 * blockingfifo.h
 *
 *  Created on: Dec 5, 2012
 *      Author: chng
 */

#ifndef BLOCKINGFIFO_H_
#define BLOCKINGFIFO_H_

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "linux_list.h"

typedef struct list_head list_head;
typedef struct list_node list_node;

#define QUEUE_HEADCNT_DEFAULT 4096

typedef struct {
	list_head head;
	void * data;
	uint64_t flags;
} QueueEntry;

typedef struct {
	list_head head;
	list_head res_head;
	QueueEntry * space;
	uint32_t space_size;
	uint32_t cnt;
	uint32_t rescnt;
	pthread_mutex_t lock;
	pthread_cond_t not_empty;
} Queue;


static inline void queue_resize_nolock(Queue * q, uint32_t size) {
	int i;
	QueueEntry * newspace = malloc(size);
	list_head newhead, newres_head;
	INIT_LIST_HEAD(&newhead);
	INIT_LIST_HEAD(&newres_head);
	for (i = 0; i < q->cnt; i++) {
		QueueEntry * entry = list_first_entry(&q->head, QueueEntry, head);
		newspace[i].data = entry->data;
		newspace[i].flags = entry->flags;
		list_add_tail(&newspace[i].head, &newhead);
		list_del(&entry->head);
	}
	free(q->space);
	list_add(&q->head, &newhead);
	list_del(&newhead);
	// q->head = newhead;

	q->space = newspace;
	q->rescnt += size / sizeof(QueueEntry) - q->cnt;
	for (i = q->cnt; i < size / sizeof(QueueEntry); i++) {
		list_add_tail(&q->space[i].head, &newres_head);
	}
	list_add(&q->res_head, &newres_head);
	list_del(&newres_head);
	// q->res_head = newres_head;
	q->space_size = size;
}

static inline void queue_push(void * data, Queue * q) {
	pthread_mutex_lock(&q->lock);
	if (!q->rescnt) {
		// Make it double
		queue_resize_nolock(q, q->space_size << 1);
	}
	QueueEntry * entry = list_first_entry(&q->res_head, QueueEntry, head);
	entry->data = data;
	list_del(&entry->head);
	list_add_tail(&entry->head, &q->head);

	q->cnt++;
	q->rescnt--;
	pthread_cond_signal(&q->not_empty);
	pthread_mutex_unlock(&q->lock);
}

static inline void * queue_pop(Queue * q) {
	void * data;
	pthread_mutex_lock(&q->lock);
	while (!q->cnt) {
		pthread_cond_wait(&q->not_empty, &q->lock);
	}
	QueueEntry * entry = list_first_entry(&q->head, QueueEntry, head);
	data = entry->data;
	list_del(&entry->head);
	list_add_tail(&entry->head, &q->res_head);

	q->cnt--;
	q->rescnt++;
	pthread_mutex_unlock(&q->lock);
	return data;
}

static inline Queue * queue_create() {
	int ret, i;
	Queue * q = malloc(sizeof(Queue));

	ret = pthread_mutex_init(&q->lock, NULL);
	if (ret) {
		perror("Mutex initialization failed");
		return NULL;
	}
	pthread_cond_init(&q->not_empty, NULL);
	INIT_LIST_HEAD(&q->head);
	INIT_LIST_HEAD(&q->res_head);
	q->space = malloc(QUEUE_HEADCNT_DEFAULT * sizeof(QueueEntry));
	q->space_size = QUEUE_HEADCNT_DEFAULT * sizeof(QueueEntry);
	q->cnt = 0;
	q->rescnt = QUEUE_HEADCNT_DEFAULT;
	for (i = 0; i < q->rescnt; i++) {
		list_add(&q->space[i].head, &q->res_head);
	}
	return q;
}

static inline void queue_destroy(Queue * q) {
	free(q->space);
	free(q);
}

#endif /* BLOCKINGFIFO_H_ */
