/*
 * hashtable.h
 *
 *  Created on: Jun 11, 2013
 *      Author: chng
 *      It is to create a hash table over an array
 */

#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

typedef struct {
	uint64_t index;
	uint32_t d1;
	uint32_t d2;
} HEntry;

typedef struct {
	void * baseptr;
	uint64_t size;
	uint32_t objsize;
	uint32_t offset;
	pthread_spinlock_t lock[256];
	HEntry * en;
} HTable;

#define HT_CREATE(size, baseptr, type, member) HTCreate(size, baseptr, sizeof(type), offsetof(type, member))

HTable * HTCreate(uint64_t keys, void * baseptr, uint32_t objsize, uint32_t offset);
HEntry * HTSearch(HTable * ht, uint8_t * key, uint32_t len);
void HTInsert(HTable * ht, uint64_t index, uint32_t d1, uint32_t d2);
void HTDelete(HTable * ht, uint64_t index);
void HTDestroy(HTable * ht);


#endif /* HASHTABLE_H_ */
