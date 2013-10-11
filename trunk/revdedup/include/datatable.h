/**
 * @file 	datatable.h
 * @brief	Data Table containing pointer for segment buffer in restoration
 * @author	Ng Chun Ho
 */

#ifndef DATATABLE_H_
#define DATATABLE_H_

#include <stdlib.h>

/**
 * Data entry definition
 */
typedef struct {
	void * data;				/*!< Pointer to segment data */
	uint64_t cnt;				/*!< Reference count of segment in the image being restored */
	pthread_mutex_t mutex;		/*!< Avoid sending out data before it is loaded and decompressed */
	pthread_spinlock_t lock;	/*!< Allow only one thread to load and decompress the segment data */
	uint32_t size;				/*!< Uncompressed segment size */
} DataEntry;

typedef struct {
	uint64_t dsize;				/*!< Data entry count */
	DataEntry * en;				/*!< Data entries */
} DataTable;

/**
 * Create a Data Table
 * @param dsize	Data entry count
 * @return	Data table
 */
static inline DataTable * NewDataTable(uint64_t dsize) {
	DataTable * dt = malloc(sizeof(DataTable));
	dt->dsize = dsize;
	dt->en = MMAP_MM(dt->dsize * sizeof(DataEntry));
	memset(dt->en, 0, dt->dsize * sizeof(DataEntry));
	return dt;
}

/**
 * Destroy a Data Table
 * @param dt	Data Table to destroy
 */
static inline void DelDataTable(DataTable * dt) {
	munmap(dt->en, dt->dsize * sizeof(DataEntry));
	free(dt);
}

#endif /* DATATABLE_H_ */
