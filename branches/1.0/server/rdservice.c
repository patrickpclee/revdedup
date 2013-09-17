/*
 * tdservice.c
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#include "rdserver.h"
#include "rdservice.h"
#include "httpservice.h"
#include "database.h"
#include "storage.h"
#include "segment.h"
#include "revdedup.h"
#include "vmrebuilder.h"

static TDService service;

static void start() {
	HTTPService * hs = getHTTPService();
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	RevDedupService * rds = getRevDedupService();
	VMRebuilderService * vms = getVMRebuilderService();
	char ** loc = malloc(sizeof(char *));
	loc[0] = "./data";
	printf("Starting all services ... ");
	sts->start(1, loc);
	dbs->start();
	rds->start();
	vms->start();
	hs->start(PORT);

	printf("Done\n");
}


static void stop() {
	HTTPService * hs = getHTTPService();
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	RevDedupService * rds = getRevDedupService();
	VMRebuilderService * vms = getVMRebuilderService();
	printf("Stopping all services ... ");
	
	hs->stop();
	vms->stop();
	rds->stop();
	dbs->stop();
	sts->stop();
	printf("Done\n");
}

static int segmentExist(const char * name, uint32_t cnt, Fingerprint * fps, char * exist) {
	uint32_t i;
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	Image * img = dbs->findImage(name);
	ImageVersion * iv = NULL;
	if (img) {
		iv = dbs->findImageVersion(img, img->versions - 1);
	}
	for (i = 0; i < cnt; i++) {
		Segment * seg = dbs->findSegment(&fps[i]);
		exist[i] = (seg != NULL) && (seg->refcnt >= 0);
		if (iv && !FP_EQUAL(&fps[i], &iv->segmentfps[i])) {
			Segment * prev_seg = dbs->findSegment(&iv->segmentfps[i]);
			sts->prefetchSegmentStore(prev_seg);
			sts->prefetchBlockFPStore(prev_seg);
		}
	}
	return 0;
}

static int putSegmentFP(Fingerprint * segmentfp, uint32_t cnt, Fingerprint * blockfps) {
	StorageService * sts = getStorageService();
	sts->createStores(segmentfp, cnt, blockfps);
	return 0;
}

pthread_mutex_t insertseg_lock = PTHREAD_MUTEX_INITIALIZER;

static int putSegmentData(Fingerprint * segmentfp, uint32_t size, void * data) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();

	pthread_mutex_lock(&insertseg_lock);
	Segment * seg = dbs->findSegment(segmentfp);
	if (!seg) {
		seg = newSegment(segmentfp, 0, size);
		dbs->insertSegment(seg);
	} else {
		if (seg->refcnt >= 0) {
			pthread_mutex_unlock(&insertseg_lock);
			return 0;
		} else {
			printf("Re-uploading ");
			printhex(&seg->fp);
			seg->refcnt = 0;
		}
	}
	pthread_mutex_unlock(&insertseg_lock);

	BlockFPStore * bs = sts->getBlockFPStore(seg);
	SegmentStore * ss = sts->getSegmentStore(seg);
	if (!bs || !ss) {
		printf("Cannot get bs %p or ss %p\n", bs, ss);
		free(seg);
		return -1;
	}
	sts->writeSegment(ss, bs, data);
	sts->putSegmentStore(ss);
	sts->putBlockFPStore(bs);
	return 0;
}

static int putImage(const char * name, uint64_t size, uint32_t cnt,
		Fingerprint * segmentfps) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	Image * img = dbs->findImage(name);
	ImageVersion * this_iv, *prev_iv = NULL;
	int i;

	// Update reference count for segments
	for (i = 0; i < cnt; i++) {
		Segment * seg = dbs->findSegment(&segmentfps[i]);
		if (!seg) {
			printf("cannot get segment ");
			printhex(&segmentfps[i]);
			return -1;
		}
		seg->refcnt++;
		dbs->updateSegment(seg);
	}
	if (!img) {
		img = newImage(name);
		dbs->insertImage(img);
	} else {
		prev_iv = dbs->findImageVersion(img, img->versions - 1);
	}

	this_iv = newImageVersion(img, size, cnt, segmentfps);
	dbs->insertImageVersion(this_iv);
	dbs->updateImage(img);

	if (!prev_iv) {
		goto end;
	}
	for (i = 0; i < prev_iv->seg_cnt; i++) {
		Segment * seg = dbs->findSegment(&prev_iv->segmentfps[i]);
		if (!seg) {
			printf("cannot get segment ");
			printhex(&prev_iv->segmentfps[i]);
			return -1;
		}
		seg->refcnt--;
		dbs->updateSegment(seg);
	}
	sync();
	RevDedupService * rds = getRevDedupService();
	VersionMap * map = sts->createVersionMap(prev_iv);
	RevDedup * rd = rds->newRevDedup(this_iv, prev_iv);

	rds->buildIndex(rd, map);

	rds->run(rd, map);

	sts->putVersionMap(map);
	rds->destroyRevDedup(rd);
end:
	while(getStorageService()->segment_punch_q->cnt);
	sync();
	return 0;
}

typedef struct {
	ImageVersion * iv;
	int fd;
	int flags;
} SendImageArgs;

static void * sendImage(void * cls) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	SendImageArgs * sia = (SendImageArgs *)cls;
	ImageVersion * iv = sia->iv;
	int fd = sia->fd;
	uint32_t i;

	for (i = 0; i < min(iv->seg_cnt, SEGMENT_PREFETCH_AHEAD); i++) {
		Segment * seg = dbs->findSegment(&iv->segmentfps[i]);
		sts->prefetchSegmentStore(seg);
	}

	for (i = 0; i < iv->seg_cnt; i++) {
		if (i + SEGMENT_PREFETCH_AHEAD < iv->seg_cnt) {
			Segment * seg = dbs->findSegment(
					&iv->segmentfps[i + SEGMENT_PREFETCH_AHEAD]);
			sts->prefetchSegmentStore(seg);
		}
		Segment * seg = dbs->findSegment(&iv->segmentfps[i]);
		SegmentStore * ss = sts->getSegmentStore(seg);
		sts->pipeSegment(ss, fd, 0, seg->size / BLOCK_SIZE);
		sts->putSegmentStore(ss);
	}

	close(sia->fd);
	free(sia);

	pthread_exit(NULL);
	return NULL;
}

static int getImage(const char * name, int fd, uint64_t * size) {
	DBService * dbs = getDBService();
	Image * img = dbs->findImage(name);
	ImageVersion * iv = NULL;
	pthread_t thread;
	SendImageArgs * sia;
	if (!img) {
		return -1;
	}
	iv = dbs->findImageVersion(img, img->versions - 1);
	if (!iv) {
		return -1;
	}

	*size = iv->size;

	sia = malloc(sizeof(SendImageArgs));
	sia->iv = iv;
	sia->fd = fd;
	pthread_create(&thread, NULL, sendImage, sia);

	return 0;
}

static void * sendImageVersion(void * cls) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();
	VMRebuilderService * vms = getVMRebuilderService();
	SendImageArgs * sia = (SendImageArgs *)cls;
	ImageVersion * iv = sia->iv;
	int fd = sia->fd;
	uint32_t versions = iv->img->versions - iv->version;
	uint32_t blocks = iv->size / BLOCK_SIZE;
	uint32_t i, j;
	uint32_t seeks = 0;

	// Work with most recent version
	if (iv->version == iv->img->versions - 1) {
		sendImage(cls);
		return NULL;
	}

	VersionMapRebuilder * vmr = vms->newVersionMapRebuilder(iv);

	for (i = 0; i < versions; i++) {
		for (j = 0; j < min(vmr->ivs[i]->seg_cnt, SEGMENT_PREFETCH_AHEAD); j++) {
			Segment * seg = dbs->findSegment(&vmr->ivs[i]->segmentfps[j]);
			sts->prefetchSegmentStore(seg);
		}
	}

	Segment * s_iseg = NULL;
	uint32_t s_iblk = 0, s_cnt = 0;

	for (i = 0; i < blocks; i++) {
		if (i % SEG_BLOCKS == 0) {
			uint32_t segnr = i / SEG_BLOCKS + SEGMENT_PREFETCH_AHEAD;
			for (j = 0; j < versions; j++) {
				if (segnr < vmr->ivs[j]->seg_cnt) {
					Segment * seg = dbs->findSegment(
							&vmr->ivs[j]->segmentfps[segnr]);
					sts->prefetchSegmentStore(seg);
				}
			}
		}
		VersionMapEntry en = vms->getVersionMapEntry(vmr, i);
		if (en.ver == -1) {
			if (s_cnt) {
				SegmentStore * ss = sts->getSegmentStore(s_iseg);
				sts->pipeSegment(ss, fd, s_iblk, s_cnt);
				sts->putSegmentStore(ss);
			}
			sts->pipeZero(fd);
			s_cnt = 0;
			continue;
		}
		ImageVersion * cur_iv = vmr->ivs[en.ver - iv->version];
		Segment * seg = dbs->findSegment(
				&cur_iv->segmentfps[en.blk / SEG_BLOCKS]);
		uint32_t iblk = en.blk % SEG_BLOCKS;

		if (s_iseg == seg && s_iblk + s_cnt == iblk) {
			s_cnt++;
			continue;
		}
		if (s_cnt) {
			SegmentStore * ss = sts->getSegmentStore(s_iseg);
			sts->pipeSegment(ss, fd, s_iblk, s_cnt);
			sts->putSegmentStore(ss);
			seeks++;
		}
		s_iseg = seg;
		s_iblk = iblk;
		s_cnt = 1;
	}
	if (s_cnt) {
		SegmentStore * ss = sts->getSegmentStore(s_iseg);
		sts->pipeSegment(ss, fd, s_iblk, s_cnt);
		sts->putSegmentStore(ss);
		seeks++;
	}

	seeks -= iv->seg_cnt;

	vms->destroyVersionMapRebuilder(vmr);
	close(sia->fd);
	free(sia);

	pthread_exit(NULL);
	return NULL;
}

static int getImageVersion(const char * name, int fd, uint64_t version,
		uint64_t * size) {
	DBService * dbs = getDBService();
	Image * img = dbs->findImage(name);
	ImageVersion * iv = NULL;
	pthread_t thread;
	SendImageArgs * sia;
	if (!img) {
		return -1;
	}
	iv = dbs->findImageVersion(img, version);
	if (!iv) {
		return -1;
	}

	*size = iv->size;

	sia = malloc(sizeof(SendImageArgs));
	sia->iv = iv;
	sia->fd = fd;
	pthread_create(&thread, NULL, sendImageVersion, sia);
	return 0;
}

TDService * getTDService() {
	return &service;
}

static TDService service  = {
		.start = start,
		.stop = stop,
		.segmentExist = segmentExist,
		.putSegmentFP = putSegmentFP,
		.putSegmentData = putSegmentData,
		.putImage = putImage,
		.getImage = getImage,
		.getImageVersion = getImageVersion,
};
