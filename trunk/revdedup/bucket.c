/*
 * bucket.c
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#include "bucket.h"
#include "fingerprint.h"
#include "index.h"
#include "image.h"

static BucketService service;

static Bucket * NewBucket(uint64_t sid) {
	char buf[64];
	Bucket * b = malloc(sizeof(Bucket));
	b->id = ++service._log->bucketID;
	b->sid = sid;
	b->segs = 0;
	b->size = 0;

	sprintf(buf, DATA_DIR "bucket/%08lx", b->id);
	b->fd = creat(buf, 0644);
	assert(b->fd != -1);
	return b;
}

static void SaveBucket(Bucket * b) {
	ssize_t remain = (BLOCK_SIZE - (b->size % BLOCK_SIZE)) % BLOCK_SIZE;
	assert(write(b->fd, service._padding, remain) == remain);
	close(b->fd);

	service._en[b->id].sid = b->sid;
	service._en[b->id].segs = b->segs;
	service._en[b->id].size = b->size + remain;
	service._en[b->id].psize = 0;
	service._en[b->id].ver = -1;	// Infinity
	service._en[b->id].inst = service._inst;
	service._en[b->id].vers = service._ver;
	free(b);
}

static Bucket * BucketInsert(Bucket * b, Segment * seg) {
	if (b == NULL) {
		b = NewBucket(seg->id);
	}
	if (b->size + seg->clen > BUCKET_SIZE) {
		SaveBucket(b);
		b = NewBucket(seg->id);
	}

	seg->pos = b->size;
	GetIndexService()->putSegment(seg, b->id);
	assert(write(b->fd, seg->cdata, seg->clen) == seg->clen);
	b->segs++;
	b->size += seg->clen;

	return b;
}

static void * process(void * ptr) {
	Bucket * b = NULL;
	Segment * seg = NULL;
	int turn = 0;
	while ((seg = (Segment *) Dequeue(service._iq)) != NULL) {
		if (seg->unique) {
			b = BucketInsert(b, seg);
		}
		Enqueue(service._oq, seg);
	}
	if (b != NULL) {
		SaveBucket(b);
	}
	Enqueue(service._oq, NULL);
	return NULL;
}

static int start(Queue * iq, Queue * oq) {
	int ret, fd;
	service._iq = iq;
	service._oq = oq;

	/* Load Bucket Log */
	fd = open(DATA_DIR "blog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES(sizeof(BMEntry))));
	service._en = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	service._log = (BucketLog *) service._en;
	close(fd);

	memset(service._padding, 0, BLOCK_SIZE);
	ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

static int stop() {
	int ret, i;
	ret = pthread_join(service._tid, NULL);
	munmap(service._en, MAX_ENTRIES(sizeof(BMEntry)));
	return ret;
}

static BucketService service = {
		.start = start,
		.stop = stop,
};

BucketService* GetBucketService() {
	return &service;
}
