/*
 * revrbd.c
 *
 *  Created on: 20 Jun, 2013
 *      Author: ngchunho
 */

#include "revrbd.h"
#include <linux/falloc.h>
#include <sys/sendfile.h>
#include "minilzo.h"

typedef struct {
	SMEntry * sen;
	uint8_t data[MAX_SEG_SIZE << 1];
} SimpleSegment;

typedef struct {
	BMEntry * ben;
	uint8_t data[BUCKET_SIZE];
} SimpleBucket;

static RevRbdService service;

static Bucket * NewBucket(uint64_t sid) {
	char buf[64];
	Bucket * b = malloc(sizeof(Bucket));
	b->id = ++service._blog->bucketID;
	b->sid = sid;
	b->segs = 0;
	b->size = 0;

	sprintf(buf, DATA_DIR "bucket/%08lx", b->id);
	b->fd = open(buf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	assert(b->fd != -1);
	return b;
}

static void SaveBucket(Bucket * b) {
	ssize_t remain = (BLOCK_SIZE - (b->size % BLOCK_SIZE)) % BLOCK_SIZE;
	assert(write(b->fd, service._padding, remain) == remain);
	close(b->fd);

	service._ben[b->id].sid = b->sid;
	service._ben[b->id].segs = b->segs;
	service._ben[b->id].size = b->size + remain;
	service._ben[b->id].psize = 0;
	service._ben[b->id].ver = service._ver;
	free(b);
}

static Bucket * BucketInsert(Bucket * b, SimpleSegment * sseg) {
	SMEntry * sen = sseg->sen;
	if (b == NULL) {
		b = NewBucket(sen - service._sen);
	}
	if (b->size + sen->len > BUCKET_SIZE) {
		SaveBucket(b);
		b = NewBucket(sen - service._sen);
	}

	sen->pos = b->size;
	sen->bucket = b->id;

	assert(write(b->fd, sseg->data, sen->len) == sen->len);
	b->segs++;
	b->size += sen->len;

	return b;
}

static void * saveSegment(void * ptr) {
	SimpleSegment * sseg;
	Bucket * b = NULL;
	int i = REV_CNT;
	while (i) {
		sseg = (SimpleSegment *) Dequeue(service._nbq);
		if (sseg == NULL) {
			i--;
			continue;
		}
		b = BucketInsert(b, sseg);
		free(sseg);
	}
	if (b) {
		SaveBucket(b);
	}
	return NULL ;
}

static void * rebuildSegments(void * ptr) {
	uint8_t buf[LZO1X_MEM_COMPRESS];
	SimpleSegment * sseg;
	uint8_t * temp = MMAP_MM(MAX_SEG_SIZE);
	uint64_t i;
	while ((sseg = (SimpleSegment *) Dequeue(service._rsq)) != NULL ) {
		SMEntry * sen = sseg->sen;
		uint64_t pos = 0, len;
#ifdef DISABLE_COMPRESSION
		memcpy(temp, sseg->data, sen->len);

#else
		if (sen->compressed) {
			lzo1x_decompress(sseg->data, sen->len, temp, &pos, NULL);
		} else {
			memcpy(temp, sseg->data, sen->len);
		}
#endif
		for (i = sen->cid; i < sen->cid + sen->chunks; i++) {
			CMEntry * cen = &service._cen[i];
			if (cen->ref == 0) {
				cen->pos = pos;
				cen->len = 0;
			} else {
				memmove(temp + pos, temp + cen->pos, cen->len);
				cen->pos = pos;
				pos += cen->len;
			}
		}
#ifdef DISABLE_COMPRESSION
		memcpy(sseg->data, temp, pos);
		sen->len = pos;
#else
		lzo1x_1_compress(temp, pos, sseg->data, &len, buf);
		if (len >= pos) {
			memcpy(sseg->data, temp, pos);
			sen->len = pos;
			sen->compressed = 0;
		} else {
			sen->len = len;
			sen->compressed = 1;
		}
#endif
		Enqueue(service._nbq, sseg);
	}
	Enqueue(service._nbq, NULL);
	munmap(temp, MAX_SEG_SIZE);
	return NULL ;
}

static void * rebuildBucket(void * ptr) {
	SimpleBucket * sbucket = NULL, * lsbucket = NULL;
	BMEntry * ben, * lben = NULL;
	uint64_t bid, lbid = 0, i;
	char buf[128];
	while ((sbucket = (SimpleBucket *) Dequeue(service._rbq)) != NULL) {
		if (lsbucket == NULL) {
			lsbucket = sbucket;
			lben = lsbucket->ben;
			lbid = lben - service._ben;
			continue;
		}
		ben = sbucket->ben;
		bid = ben - service._ben;
		if ((ben->sid == lben->sid + lben->segs)
				&& ben->size + lben->size <= BUCKET_SIZE) {
			// Combine two adjacent buckets
			memcpy(lsbucket->data + lben->size, sbucket->data, ben->size);
			for (i = ben->sid; i < ben->sid + ben->segs; i++) {
				if (service._sen[i].bucket == bid) {
					service._sen[i].bucket = lbid;
					service._sen[i].pos += lben->size;
				}
			}
			lben->segs += ben->segs;
			lben->size += ben->size;

			sprintf(buf, DATA_DIR "bucket/%08lx", bid);
			unlink(buf);
		} else {
			sprintf(buf, DATA_DIR "bucket/%08lx", lbid);
			int fd = creat(buf, 0644);
			assert(write(fd, lsbucket->data, lben->size) == lben->size);
			close(fd);
			lsbucket = sbucket;
			lben = lsbucket->ben;
			lbid = lben - service._ben;
		}
	}
	if (lsbucket) {
		sprintf(buf, DATA_DIR "bucket/%08lx", lbid);
		int fd = creat(buf, 0644);
		assert(write(fd, lsbucket->data, lben->size) == lben->size);
		close(fd);
	}
	return NULL;
}

static void * prefetch(void * ptr) {
	BMEntry * ben;
	char buf[128];
	int fd;
	while ((ben = (BMEntry *)Dequeue(service._pfq)) != NULL) {
		sprintf(buf, DATA_DIR "bucket/%08lx", ben - service._ben);
		fd = open(buf, O_RDWR);
		posix_fadvise(fd, 0, lseek(fd, 0, SEEK_END), POSIX_FADV_WILLNEED);
		close(fd);
	}

	return NULL;
}


static void * processBuckets(void * ptr) {
	BMEntry * ben;
	uint64_t i, bid;
	char buf[128];
	while ((ben = (BMEntry *) Dequeue(service._bq)) != NULL ) {
		bid = ben - service._ben;

		sprintf(buf, DATA_DIR "bucket/%08lx", bid);
		int fd = open(buf, O_RDWR);
		// Rebuild Buckets
		if (ben->psize > MAX_PUNCH(ben->size)) {
			SimpleBucket * sbucket = malloc(sizeof(SimpleBucket));
			sbucket->ben = ben;
			int64_t pos = 0, offset = 0;
			for (i = ben->sid; i < ben->sid + ben->segs; i++) {
				SMEntry * sen = &service._sen[i];
				if (sen->bucket != bid) {
					continue;
				}
				if (sen->ref == -1) {
					SimpleSegment * sseg = malloc(sizeof(SimpleSegment));
					sseg->sen = sen;
					assert(pread(fd, sseg->data, sen->len, sen->pos) == sen->len);
					Enqueue(service._rsq, sseg);
				} else {
					assert(pread(fd, sbucket->data + pos, sen->len, sen->pos) == sen->len);
					sen->pos = pos;
					pos += sen->len;
				}
			}
			ben->size = pos;
			ben->psize = 0;
			Enqueue(service._rbq, sbucket);
		} else {
			uint32_t pos = 0, end = 0;
			for (i = ben->sid; i < ben->sid + ben->segs; i++) {
				SMEntry * sen = &service._sen[i];
				if (sen->bucket != bid) {
					continue;
				}
				if (sen->ref == -1) {
					SimpleSegment * sseg = malloc(sizeof(SimpleSegment));
					sseg->sen = sen;
					assert(pread(fd, sseg->data, sen->len, sen->pos) == sen->len);
					Enqueue(service._rsq, sseg);
				} else {
					end = sen->pos / BLOCK_SIZE * BLOCK_SIZE;
					if (end > pos) {
						fallocate(fd, 0x03, pos, end - pos);
					}
					pos = (sen->pos + sen->len + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
				}
			}
			end = ben->size / BLOCK_SIZE * BLOCK_SIZE;
			if (end > pos) {
				fallocate(fd, 0x03, pos, end - pos);
			}
		}

		close(fd);
	}
	Enqueue(service._rbq, NULL);
	for (i = 0; i < REV_CNT; i++) {
		Enqueue(service._rsq, NULL);
	}
	return NULL ;
}

static void * process(void * ptr) {
	uint64_t i, j;
	for (i = 1; i <= service._blog->bucketID; i++) {
		BMEntry * ben = &service._ben[i];
		uint32_t psize = 0;
		/*
		 * TODO : the following for-loop is a critical section to the
		 * normal convdedup routine: we remove keys in the index and
		 * set references to -1. Therefore we should take a global
		 * lock so that normal convdedup cannot be running.
		 */
		for (j = ben->sid; j < ben->sid + ben->segs; j++) {
			SMEntry * sen = &service._sen[j];
			if (sen->ref != 0) {
				continue;
			}
			kcdbremove(service.__db, (char *)sen->fp, FP_SIZE);
			psize += sen->len;
			sen->ref = -1;
		}

		if (!psize) {
			continue;
		}
		ben->psize += psize;
		if (ben->psize > MAX_PUNCH(ben->size)) {
			Enqueue(service._pfq, ben);
		}
		Enqueue(service._bq, ben);
	}
	Enqueue(service._pfq, NULL);
	Enqueue(service._bq, NULL);
	return NULL;
}

static int start(SMEntry * sen, CMEntry * cen, BMEntry * ben, uint32_t ver) {
	int ret, i;

	service._bq = NewQueue();
	service._rsq = NewQueue();
	service._nbq = NewQueue();
	service._pfq = NewQueue();
	service._rbq = NewQueue();

	service._sen = sen;
	service._slog = (SegmentLog *) sen;
	service._cen = cen;
	service._clog = (ChunkLog *) cen;
	service._ben = ben;
	service._blog = (BucketLog *) ben;

	service._ver = ver;

	service.__db = kcdbnew();
	kcdbopen(service.__db, "-", KCOWRITER | KCOCREATE);
	kcdbloadsnap(service.__db, DATA_DIR "index");

	memset(service._padding, 0, BLOCK_SIZE);

	ret = pthread_create(&service._tid, NULL, process, NULL);
	ret = pthread_create(&service._pbid, NULL, processBuckets, NULL);
	for (i = 0; i < REV_CNT; i++) {
		ret = pthread_create(&service._rsid[i], NULL, rebuildSegments, NULL);
	}
	ret = pthread_create(&service._ssid, NULL, saveSegment, NULL);
	ret = pthread_create(&service._pfid, NULL, prefetch, NULL);
	ret = pthread_create(&service._rbid, NULL, rebuildBucket, NULL);
	return ret;
}

static int stop() {
	int ret = 0, i;
	pthread_join(service._tid, NULL );
	pthread_join(service._pbid, NULL );
	for (i = 0; i < REV_CNT; i++) {
		pthread_join(service._rsid[i], NULL );
	}
	pthread_join(service._ssid, NULL );
	pthread_join(service._pfid, NULL );
	pthread_join(service._rbid, NULL );

	kcdbdumpsnap(service.__db, DATA_DIR "index");
	kcdbdel(service.__db);

	free(service._bq);
	free(service._rsq);
	free(service._nbq);
	free(service._pfq);
	free(service._rbq);
	return ret;
}

static RevRbdService service = {
		.start = start,
		.stop = stop,
};


RevRbdService * GetRevRbdService() {
	return &service;
}
