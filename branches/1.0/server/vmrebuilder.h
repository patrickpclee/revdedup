/*
 * vmrebuilder.h
 *
 *  Created on: Dec 21, 2012
 *      Author: chng
 */

#ifndef VMREBUILDER_H_
#define VMREBUILDER_H_

#define VMAP_ZERO 0xFFFFFFFF
#define VMAP_THIS 0xFFFFFFFE

#include "rdserver.h"
#include "image.h"
#include "storage.h"
#include <sys/time.h>

typedef struct {
	int32_t ver;
	uint32_t blk;
} VersionMapEntry;

typedef struct {
	uint32_t vcnt;
	volatile uint32_t ptr;
	ImageVersion ** ivs;
	VersionMap ** maps;
	VersionMapEntry * vmap;
	pthread_mutex_t lock;
	struct timeval t;
} VersionMapRebuilder;

typedef struct {
	pthread_t rebuild_t;
	pthread_mutex_t rebuild_lock;
	pthread_cond_t rebuild_cond;
	VersionMapRebuilder * vmr;
	VersionMapRebuilder * (*newVersionMapRebuilder)(ImageVersion * iv);
	VersionMapEntry (*getVersionMapEntry)(VersionMapRebuilder * vmr, uint32_t lblk);
	void (*destroyVersionMapRebuilder)(VersionMapRebuilder * vmr);
	void (*start)();
	void (*stop)();
} VMRebuilderService;

VMRebuilderService * getVMRebuilderService();


#endif /* VMREBUILDER_H_ */
