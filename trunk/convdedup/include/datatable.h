/*
 * datatable.h
 *
 *  Created on: Aug 5, 2013
 *      Author: chng
 */

#ifndef DATATABLE_H_
#define DATATABLE_H_

#include <stdlib.h>

typedef struct {
	void * data;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	volatile uint32_t cnt;
	volatile uint32_t size;
} DataEntry;

typedef struct {
	uint64_t dsize;
	DataEntry * en;
} DataTable;

static inline DataTable * dt_create(uint64_t dsize) {
	DataTable * dt = malloc(sizeof(DataTable));
	dt->dsize = dsize;
	dt->en = MMAP_MM(dt->dsize * sizeof(DataEntry));
	// memset(dt->en, 0, dt->dsize * sizeof(DataEntry));
	return dt;
}

static inline void dt_destroy(DataTable * dt) {
	munmap(dt->en, dt->dsize * sizeof(DataEntry));
	free(dt);
}

#endif /* DATATABLE_H_ */
