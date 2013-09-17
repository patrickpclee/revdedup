/*
 * hashtable.c
 *
 *  Created on: Jun 11, 2013
 *      Author: chng
 */

#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include "hashtable.h"


HTable * HTCreate(uint64_t size, void * baseptr, uint32_t objsize, uint32_t offset) {
	HTable * ht = malloc(sizeof(HTable));
	ht->baseptr = baseptr;
	ht->size = size;
	ht->objsize = objsize;
	ht->offset = offset;
	ht->en = mmap(0, ht->size * sizeof(HEntry), 0x3, 0x2 | MAP_ANONYMOUS, -1, 0);
	memset(ht->en, 0xFF, ht->size * sizeof(HEntry));
	int i;
	for (i = 0; i < 256; i++) {
		pthread_spin_init(ht->lock + i, PTHREAD_PROCESS_SHARED);
	}
	return ht;
}

HEntry * HTSearch(HTable * ht, uint8_t * key, uint32_t len) {
	uint8_t p = *key;
	uint64_t pos = *((uint64_t *) key) % ht->size;
	uint64_t index;
	pthread_spin_lock(ht->lock + p);
	while (ht->en[pos].index != -1) {
		index = ht->en[pos].index;
		if (!memcmp(key, ht->baseptr + index * ht->objsize + ht->offset, len)) {
			pthread_spin_unlock(ht->lock + p);
			return &ht->en[pos];
		}
		pos++;
		if (pos == ht->size) {
			pos = 0;
		}
	}

	pthread_spin_unlock(ht->lock + p);
	return NULL;
}

void HTInsert(HTable * ht, uint64_t index, uint32_t d1, uint32_t d2) {
	uint8_t p = *(uint8_t *)(ht->baseptr + index * ht->objsize + ht->offset);
	uint64_t pos = *(uint64_t *)(ht->baseptr + index * ht->objsize + ht->offset) % ht->size;
	pthread_spin_lock(ht->lock + p);
	while(ht->en[pos].index != -1) {
		if (ht->en[pos].index == index) {
			pthread_spin_unlock(ht->lock + p);
			return;
		}
		if (++pos == ht->size) {
			pos = 0;
		}
	}
	ht->en[pos].index = index;
	ht->en[pos].d1 = d1;
	ht->en[pos].d2 = d2;
	pthread_spin_unlock(ht->lock + p);
}

void HTDelete(HTable * ht, uint64_t index) {

}

void HTDestroy(HTable * ht) {
	munmap(ht->en, ht->size * sizeof(HEntry));
	free(ht);
}
