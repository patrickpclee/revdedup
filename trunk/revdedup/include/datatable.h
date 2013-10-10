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
	uint64_t cnt;
	pthread_mutex_t mutex;
	pthread_spinlock_t lock;
	uint32_t size;
} DataEntry;

typedef struct {
	uint64_t dsize;
	DataEntry * en;
} DataTable;

static inline DataTable * NewDataTable(uint64_t dsize) {
	DataTable * dt = malloc(sizeof(DataTable));
	dt->dsize = dsize;
	dt->en = MMAP_MM(dt->dsize * sizeof(DataEntry));
	memset(dt->en, 0, dt->dsize * sizeof(DataEntry));
	return dt;
}

static inline void DelDataTable(DataTable * dt) {
	munmap(dt->en, dt->dsize * sizeof(DataEntry));
	free(dt);
}

#endif /* DATATABLE_H_ */
