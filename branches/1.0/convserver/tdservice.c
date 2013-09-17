/*
 * tdservice.c
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#include "tdserver.h"
#include "tdservice.h"
#include "httpservice.h"
#include "database.h"
#include "storage.h"
#include "segment.h"


static TDService service;

static struct timeval s;
static FILE * ufp;
static FILE * dfp;
static FILE * vfp;

static inline void setTimeStamp() {
	gettimeofday(&s, NULL);
}

static inline void putTimeStamp(FILE * fp) {
	struct timeval t, r;
	gettimeofday(&t, NULL);
	timersub(&t, &s, &r);
	fprintf(fp, "%ld.%06ld\t", r.tv_sec, r.tv_usec);
	setTimeStamp();
}

static void finishTimeStamp(FILE * fp) {
	fprintf(fp, "\n");
	fflush(fp);
}

static void start() {
	HTTPService * hs = getHTTPService();
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();

	char ** loc = malloc(8 * sizeof(char *));	// 8 disks
	loc[0] = "./disk0";
	loc[1] = "./disk1";
	loc[2] = "./disk2";
	loc[3] = "./disk3";
	loc[4] = "./disk4";
	loc[5] = "./disk5";
	loc[6] = "./disk6";
	loc[7] = "./disk7";
	printf("Starting all services ... ");
	sts->start(1, loc);
	dbs->start();

	hs->start(5749);

	char buffer[STRING_BUF_SIZE];
	sprintf(buffer, "uts%d", SEG_SIZE / 1024);
	ufp = fopen(buffer, "a");
	sprintf(buffer, "dts%d", SEG_SIZE / 1024);
	dfp = fopen(buffer, "a");
	sprintf(buffer, "vts%d", SEG_SIZE / 1024);
	vfp = fopen(buffer, "a");
	printf("Done\n");
}


static void stop() {
	HTTPService * hs = getHTTPService();
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();

	printf("Stopping all services ... ");
	fclose(ufp);
	fclose(dfp);
	fclose(vfp);
	
	hs->stop();
	dbs->stop();
	sts->stop();
	printf("Done\n");
}

static int segmentExist(uint32_t cnt, Fingerprint * fps, char * exist) {
	uint32_t i;
	DBService * dbs = getDBService();

	setTimeStamp();
	for (i = 0; i < cnt; i++) {
		Segment * seg = dbs->findSegment(&fps[i]);
		exist[i] = (seg != NULL) && (seg->refcnt >= 0);
	}
	return 0;
}

static int putSegmentData(Fingerprint * segmentfp, uint32_t size, void * data) {
	DBService * dbs = getDBService();
	StorageService * sts = getStorageService();

	Segment * seg = newSegment(segmentfp, 0, size);
	if (dbs->insertSegment(seg) == -1) {
		free(seg);
		seg = dbs->findSegment(segmentfp);
		if (seg->refcnt >= 0) {
			return 0;
		}
	}
	if (seg->refcnt == -1) {
		seg->refcnt++;
	}
	SegmentStore * ss = sts->getSegmentStore(seg);
	if (!ss) {
		printf("Cannot get ss %p\n", ss);
		free(seg);
		return -1;
	}
	sts->writeSegment(ss, data);
	sts->putSegmentStore(ss);
	return 0;
}

static int putImage(const char * name, uint64_t size, uint32_t cnt,
		Fingerprint * segmentfps) {
	DBService * dbs = getDBService();
	Image * img = dbs->findImage(name);
	ImageVersion * this_iv;
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
	}


	this_iv = newImageVersion(img, size, cnt, segmentfps);
	dbs->insertImageVersion(this_iv);
	dbs->updateImage(img);

	sync();
	putTimeStamp(ufp);
	finishTimeStamp(ufp);
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
	
	Segment * segp = malloc(sizeof(Segment));
	Segment * segr = malloc(sizeof(Segment));

	setTimeStamp();

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
		memcpy(&segr->fp, &iv->segmentfps[i], FP_SIZE);
		Segment * seg = segr;
		// Segment * seg = dbs->findSegment(&iv->segmentfps[i]);
		SegmentStore * ss = sts->getSegmentStore(seg);
		sts->pipeSegment(ss, fd, 0, 1);
		sts->putSegmentStore(ss);
	}

	free(segp);
	free(segr);


	close(sia->fd);
	free(sia);


	putTimeStamp(dfp);
	finishTimeStamp(dfp);

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
	sendImage(cls);
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
		.putSegmentData = putSegmentData,
		.putImage = putImage,
		.getImage = getImage,
		.getImageVersion = getImageVersion,
};
