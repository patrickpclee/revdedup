/*
 * vmrebuilder.c
 *
 *  Created on: Dec 21, 2012
 *      Author: chng
 */

#include "rdserver.h"
#include "vmrebuilder.h"
#include "database.h"

static VMRebuilderService service;

static VersionMapRebuilder * newVersionMapRebuilder(ImageVersion * iv) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	VersionMapRebuilder * vmr = malloc(sizeof(VersionMapRebuilder));
	uint32_t i;

	vmr->vcnt = iv->img->versions - iv->version;
	vmr->ivs = malloc(vmr->vcnt * sizeof(ImageVersion *));
	vmr->maps = malloc(vmr->vcnt * sizeof(VersionMap *));
	vmr->vmap = malloc(iv->size / BLOCK_SIZE * sizeof(VersionMapEntry));
	vmr->ptr = 0;
	pthread_mutex_init(&vmr->lock, NULL);

	for (i = 0; i < vmr->vcnt - 1; i++) {
		vmr->ivs[i] = dbs->findImageVersion(iv->img, iv->version + i);
		vmr->maps[i] = sts->getVersionMap(vmr->ivs[i]);
	}
	vmr->ivs[vmr->vcnt - 1] = dbs->findImageVersion(iv->img, iv->img->versions - 1);

	pthread_mutex_lock(&vmr->lock);

	pthread_mutex_lock(&service.rebuild_lock);
	service.vmr = vmr;
	pthread_cond_signal(&service.rebuild_cond);
	pthread_mutex_unlock(&service.rebuild_lock);
	return vmr;
}

static VersionMapEntry getVersionMapEntry(VersionMapRebuilder * vmr, uint32_t lblk) {
	// Busy Wait
	while (lblk >= vmr->ptr);
	return vmr->vmap[lblk];
}

static void destroyVersionMapRebuilder(VersionMapRebuilder * vmr) {
	uint32_t i;
	StorageService * sts = getStorageService();
	pthread_mutex_lock(&vmr->lock);
	pthread_mutex_unlock(&vmr->lock);

	for (i = 0; i < vmr->vcnt - 1; i++) {
		sts->putVersionMap(vmr->maps[i]);
	}

	free(vmr->vmap);
	free(vmr->maps);
	free(vmr->ivs);
	free(vmr);
}

static void * workFn(void * cls) {
	uint32_t i, j;
	pthread_mutex_lock(&service.rebuild_lock);
	while (1) {
		pthread_cond_wait(&service.rebuild_cond, &service.rebuild_lock);

		VersionMapRebuilder * vmr = service.vmr;
		if (!vmr) {
			break;
		}

		VersionMapEntry * vmap = vmr->vmap;
		uint32_t blocks = vmr->ivs[0]->size / BLOCK_SIZE;
		for (j = 0; j < blocks; j++) {
			// This version
			if (vmr->maps[0]->ptr[j] == VMAP_THIS) {
				vmap[j].ver = vmr->maps[0]->iv->version;
				vmap[j].blk = j;
			} else if (vmr->maps[0]->ptr[j] == VMAP_ZERO) {
				vmap[j].ver = -1;
				vmap[j].blk = 0;
			} else {
				vmap[j].ver = vmr->maps[0]->iv->version + 1;
				vmap[j].blk = vmr->maps[0]->ptr[j];
			}

			for (i = 1; i < vmr->vcnt - 1; i++) {
				if (vmr->vmap[j].ver == -1 || vmr->vmap[j].ver < i + vmr->ivs[0]->version) {
					continue;
				}
				if (vmr->maps[i]->ptr[vmap[j].blk] == VMAP_THIS) {
					// Nothing to do
				} else if (vmr->maps[i]->ptr[vmap[j].blk] == VMAP_ZERO) {
					vmap[j].ver = -1;
					vmap[j].blk = 0;
				} else {
					vmap[j].ver++;
					vmap[j].blk = vmr->maps[i]->ptr[vmap[j].blk];
				}
			}
			vmr->ptr++;
		}
		gettimeofday(&vmr->t, NULL);
		pthread_mutex_unlock(&vmr->lock);
	}
	pthread_mutex_unlock(&service.rebuild_lock);
	return NULL;
}

static void start() {
	pthread_cond_init(&service.rebuild_cond, NULL);
	pthread_mutex_init(&service.rebuild_lock, NULL);
	pthread_create(&service.rebuild_t, NULL, workFn, NULL);
}

static void stop() {
	service.vmr = NULL;
	pthread_cond_signal(&service.rebuild_cond);
	pthread_join(service.rebuild_t, NULL);
}

static VMRebuilderService service = {
		.newVersionMapRebuilder = newVersionMapRebuilder,
		.getVersionMapEntry = getVersionMapEntry,
		.destroyVersionMapRebuilder = destroyVersionMapRebuilder,
		.start = start,
		.stop = stop,
};

VMRebuilderService * getVMRebuilderService() {
	return &service;
}
