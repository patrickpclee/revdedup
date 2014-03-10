/**
 * @file	revrbd.c
 * @brief	Reverse Deduplication Reconstruction Service Implementation
 * @author	Ng Chun Ho
 */

#include "revrbd_meta.h"
#include <linux/falloc.h>
#include <sys/sendfile.h>
#include "minilzo.h"

/**
 * Ties segment metadata with its data
 */
typedef struct {
	SMEntry * sen;
	uint8_t data[MAX_SEG_SIZE << 1];
} SimpleSegment;

/**
 * Ties bucket metadata with its data
 */
typedef struct {
	BMEntry * ben;
	uint8_t data[BUCKET_SIZE];
} SimpleBucket;

static RevRbdService service;

/**
 * Create a new bucket in memory
 * @param sid		Starting segment ID
 * @return			Created bucket
 */
static Bucket * NewBucket(uint64_t sid) {
	char buf[64];
	Bucket * b = malloc(sizeof(Bucket));
	b->id = ++service._blog->bucketID;
	b->sid = sid;
	b->segs = 0;
	b->size = 0;

	/*
	sprintf(buf, DATA_DIR "bucket/%08lx", b->id);
	b->fd = open(buf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	assert(b->fd != -1);
	*/
	return b;
}

/**
 * Seal the bucket on disk
 * @param b			Bucket to seal
 */
static void SaveBucket(Bucket * b) {
	/*
	ssize_t remain = (BLOCK_SIZE - (b->size % BLOCK_SIZE)) % BLOCK_SIZE;
	assert(write(b->fd, service._padding, remain) == remain);
	close(b->fd);
	*/

	service._ben[b->id].sid = b->sid;
	service._ben[b->id].segs = b->segs;
	service._ben[b->id].size = b->size;// + remain;
	service._ben[b->id].psize = 0;
	service._ben[b->id].ver = service._ver;
	free(b);
}

/**
 * Insert a segment into buckets
 * @param b			Bucket to insert
 * @param sseg		Segment to write
 * @return			Bucket for subsequent insertion
 */
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

	
	//assert(write(b->fd, sseg->data, sen->len) == sen->len);
	//Fix bucket->segs
	uint32_t sid = sen - service._sen;
	if(sid < b->sid) {
		b->segs = b->sid + b->segs - sid;
		b->sid = sid;
	}
	if(sid >= (b->sid + b->segs)) {
		b->segs = sid - b->sid + 1;
	}
	//b->segs++;
	b->size += sen->len;

	return b;
}

/**
 * Gather reconstructed segments and writes them to buckets
 * @param ptr		useless
 */
static void * saveSegment(void * ptr) {
	SimpleSegment * sseg;
	Bucket * b = NULL;
	int i = REV_CNT;
	while (i) {
		sseg = (SimpleSegment *) Dequeue(service._ssq);
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

/**
 * Removes unreferenced chunks in the segments
 * @param ptr		useless
 */
static void * rebuildSegments(void * ptr) {
	uint8_t buf[LZO1X_MEM_COMPRESS];
	SimpleSegment * sseg;
	uint8_t * temp = MMAP_MM(MAX_SEG_SIZE);
	uint64_t i;
	while ((sseg = (SimpleSegment *) Dequeue(service._rsq)) != NULL ) {
		SMEntry * sen = sseg->sen;
		uint64_t pos = 0, len;
#ifdef DISABLE_COMPRESSION
		//memcpy(temp, sseg->data, sen->len);

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
				//memmove(temp + pos, temp + cen->pos, cen->len);
				cen->pos = pos;
				pos += cen->len;
			}
		}
#ifdef DISABLE_COMPRESSION
		//memcpy(sseg->data, temp, pos);
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
		Enqueue(service._ssq, sseg);
	}
	Enqueue(service._ssq, NULL);
	munmap(temp, MAX_SEG_SIZE);
	return NULL ;
}

/**
 * Combines small buckets into a larger one
 * @param ptr		useless
 */
static void * combineBuckets(void * ptr) {
	SimpleBucket * sbucket = NULL, * lsbucket = NULL;
	BMEntry * ben, * lben = NULL;
	uint64_t bid, lbid = 0, i;
	char buf[128];
	FILE * bfd = fopen("bucket_combine.log","a");
	while ((sbucket = (SimpleBucket *) Dequeue(service._cbq)) != NULL) {
		if (lsbucket == NULL) {
			lsbucket = sbucket;
			lben = lsbucket->ben;
			lbid = lben - service._ben;
			continue;
		}
		ben = sbucket->ben;
		bid = ben - service._ben;

		//fprintf(stderr,"LBucket: %08lx, Bucket: %08lx\n",lbid,bid);
		if ((ben->sid == lben->sid + lben->segs)
				&& ben->size + lben->size <= BUCKET_SIZE) {
			// Combine two adjacent buckets
			//memcpy(lsbucket->data + lben->size, sbucket->data, ben->size);
			for (i = ben->sid; i < ben->sid + ben->segs; i++) {
				if (service._sen[i].bucket == bid) {
					service._sen[i].bucket = lbid;
					service._sen[i].pos += lben->size;
				}
			}
			lben->segs += ben->segs;
			lben->size += ben->size;
			
			/*
			sprintf(buf, DATA_DIR "bucket/%08lx", bid);
			int tmp_fd = open(buf, O_RDONLY);
			fprintf(bfd,"Bucket: %08lx, %ld; ", bid,lseek(tmp_fd,0,SEEK_END));
			close(tmp_fd);
			unlink(buf);
			*/
		} else {
			/* Write buckets when it is full
			sprintf(buf, DATA_DIR "bucket/%08lx", lbid);
			int fd = creat(buf, 0644);
			assert(write(fd, lsbucket->data, lben->size) == lben->size);
			close(fd);
			*/
			lsbucket = sbucket;
			lben = lsbucket->ben;
			lbid = lben - service._ben;
		}
	}
	if (lsbucket) {
		/*
		sprintf(buf, DATA_DIR "bucket/%08lx", lbid);
		int fd = creat(buf, 0644);
		assert(write(fd, lsbucket->data, lben->size) == lben->size);
		close(fd);
		*/
	}
	fprintf(bfd,"\n");
	//close(bfd);
	return NULL;
}

/**
 * Bucket prefetching routine
 * @param ptr		useless
 */
static void * prefetch(void * ptr) {
	BMEntry * ben;
	char buf[128];
	int fd;
	while ((ben = (BMEntry *)Dequeue(service._pfq)) != NULL) {
		/*
		sprintf(buf, DATA_DIR "bucket/%08lx", ben - service._ben);
		fd = open(buf, O_RDONLY);
		posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
		close(fd);
		*/
	}

	return NULL;
}

/**
 * Packs buckets by removing segment with 0 reference count
 * @param ptr		useless
 */
static void * packBuckets(void * ptr) {
	BMEntry * ben;
	uint64_t i, bid;
	char buf[128];
	while ((ben = (BMEntry *) Dequeue(service._pbq)) != NULL ) {
		bid = ben - service._ben;

		/*
		sprintf(buf, DATA_DIR "bucket/%08lx", bid);
		int fd = open(buf, O_RDWR);
		int fsize = lseek(fd,0,SEEK_END);
		*/
		if (ben->psize > MAX_PUNCH(ben->size)) {
			// Reconstruct buckets
			SimpleBucket * sbucket = malloc(sizeof(SimpleBucket));
			sbucket->ben = ben;
			int64_t pos = 0, offset = 0;
			//fprintf(stderr,"Packet Bucket %08lx\n",bid);
			for (i = ben->sid; i < ben->sid + ben->segs; i++) {
				SMEntry * sen = &service._sen[i];
				if (sen->bucket != bid) {
					continue;
				}
				if (sen->ref == -1) {
					SimpleSegment * sseg = malloc(sizeof(SimpleSegment));
					sseg->sen = sen;
					//assert(pread(fd, sseg->data, sen->len, sen->pos) == sen->len);

					/*int tmp = pread(fd, sseg->data, sen->len, sen->pos);
					if (tmp != sen->len){
						fprintf(stderr,"REVRBD: %d, %p, %ld, %ld, %d,%d, %d\n",fd,sseg->data, sen->len, sen->pos,service._ben[sen->bucket].size,fsize,tmp);
						exit(0);
					}*/
					//invalidate the bucket id for unreferenced segments
					sen->bucket = -1;
					Enqueue(service._rsq, sseg);
				} else {
					//assert(pread(fd, sbucket->data + pos, sen->len, sen->pos) == sen->len);

					/*int tmp = pread(fd, sbucket->data + pos, sen->len, sen->pos);
					if (tmp != sen->len){
						fprintf(stderr,"REVRBD: %d, %p, %ld, %ld,%d,%d,%d\n",fd,sbucket->data+pos, sen->len, sen->pos,service._ben[sen->bucket].size,fsize,tmp);
						exit(0);
					}*/
					sen->pos = pos;
					pos += sen->len;
				}
			}
			ben->size = pos;
			ben->psize = 0;
			Enqueue(service._cbq, sbucket);
		} else {
			// Punching holes in buckets
			uint32_t pos = 0, end = 0;
			for (i = ben->sid; i < ben->sid + ben->segs; i++) {
				SMEntry * sen = &service._sen[i];
				if (sen->bucket != bid) {
					continue;
				}
				if (sen->ref == -1) {
					SimpleSegment * sseg = malloc(sizeof(SimpleSegment));
					sseg->sen = sen;
					//assert(pread(fd, sseg->data, sen->len, sen->pos) == sen->len);
					Enqueue(service._rsq, sseg);
				} else {
					end = sen->pos / BLOCK_SIZE * BLOCK_SIZE;
					/*
					if (end > pos) {
						fallocate(fd, 0x03, pos, end - pos);
					}*/
					pos = (sen->pos + sen->len + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
				}
			}
			end = ben->size / BLOCK_SIZE * BLOCK_SIZE;
			/*
			if (end > pos) {
				fallocate(fd, 0x03, pos, end - pos);
			}
			*/
		}

		//close(fd);
	}
	Enqueue(service._cbq, NULL);
	for (i = 0; i < REV_CNT; i++) {
		Enqueue(service._rsq, NULL);
	}
	return NULL ;
}

/**
 * Main loop to scan buckets that contains segments for reconstruction
 * @param ptr			useless
 */
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
			kcdbremove(service._db, (char *)sen->fp, FP_SIZE);
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
		Enqueue(service._pbq, ben);
	}
	Enqueue(service._pfq, NULL);
	Enqueue(service._pbq, NULL);
	return NULL;
}

/**
 * Implements RevRbdService->start()
 */
static int start(SMEntry * sen, CMEntry * cen, BMEntry * ben, uint32_t ver) {
	int ret, i;

	service._pbq = NewQueue();
	service._rsq = NewQueue();
	service._ssq = NewQueue();
	service._pfq = NewQueue();
	service._cbq = NewQueue();

	service._sen = sen;
	service._slog = (SegmentLog *) sen;
	service._cen = cen;
	service._clog = (ChunkLog *) cen;
	service._ben = ben;
	service._blog = (BucketLog *) ben;

	service._ver = ver;

	service._db = kcdbnew();
	kcdbopen(service._db, "-", KCOWRITER | KCOCREATE);
	kcdbloadsnap(service._db, DATA_DIR "index");

	memset(service._padding, 0, BLOCK_SIZE);

	ret = pthread_create(&service._tid, NULL, process, NULL);
	ret = pthread_create(&service._pbid, NULL, packBuckets, NULL);
	for (i = 0; i < REV_CNT; i++) {
		ret = pthread_create(&service._rsid[i], NULL, rebuildSegments, NULL);
	}
	ret = pthread_create(&service._ssid, NULL, saveSegment, NULL);
	ret = pthread_create(&service._pfid, NULL, prefetch, NULL);
	ret = pthread_create(&service._cbid, NULL, combineBuckets, NULL);
	return ret;
}

/**
 * Implements RevRbdService->stop()
 */
static int stop() {
	int ret = 0, i;
	pthread_join(service._tid, NULL );
	pthread_join(service._pbid, NULL );
	for (i = 0; i < REV_CNT; i++) {
		pthread_join(service._rsid[i], NULL );
	}
	pthread_join(service._ssid, NULL );
	pthread_join(service._pfid, NULL );
	pthread_join(service._cbid, NULL );

	kcdbdumpsnap(service._db, DATA_DIR "index");
	kcdbclose(service._db);
	kcdbdel(service._db);

	free(service._pbq);
	free(service._rsq);
	free(service._ssq);
	free(service._pfq);
	free(service._cbq);
	return ret;
}

static RevRbdService service = {
		.start = start,
		.stop = stop,
};


RevRbdService * GetRevRbdService() {
	return &service;
}
