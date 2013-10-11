/**
 * @file	index.c
 * @brief	Index Service Implementation
 * @author	Ng Chun Ho
 */

#include "index.h"

static IndexService service;

/**
 * Distribute Segment and Chunk ID, also sets database entry
 * @param seg		Segment to process
 */
static inline void setDatabase(Segment * seg) {
	seg->unique = 1;
	seg->id = ++service._slog->segID;
	seg->cid = service._clog->chunkID + 1;
	kcdbadd(service._db, (char *)seg->fp, FP_SIZE, (char *)&seg->id, sizeof(uint64_t));
	service._clog->chunkID += seg->chunks;
}

/**
 * Main loop for processing segments
 * @param ptr		useless
 */
static void * process(void * ptr) {
	uint32_t i;
	Segment * seg;
	uint64_t size;
	while ((seg = (Segment *) Dequeue(service._iq)) != NULL) {
		if (DISABLE_BLOOM || bloom_add(&service._bl, seg->fp, FP_SIZE)) {
			void * idptr = kcdbget(service._db, (char *)seg->fp, FP_SIZE, &size);
			if (idptr == NULL) {
				setDatabase(seg);
			} else {
				seg->id = *(uint64_t *)idptr;
				seg->unique = 0;
			}
		} else {
			setDatabase(seg);
		}
		service._sen[seg->id].ref++;
		Enqueue(service._oq, seg);
	}
	Enqueue(service._oq, NULL );
	return NULL ;
}

/**
 * Implements BucketService->start()
 */
static int start(Queue * iq, Queue * oq) {
	int fd, i, ret;
	service._iq = iq;
	service._oq = oq;

	/* Read Chunk Metadata */
	fd = open(DATA_DIR "slog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES(sizeof(SMEntry))));
	service._sen = MMAP_FD(fd, MAX_ENTRIES(sizeof(SMEntry)));
	service._slog = (SegmentLog *) service._sen;
	close(fd);

	fd = open(DATA_DIR "clog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES(sizeof(CMEntry))));
	service._cen = MMAP_FD(fd, MAX_ENTRIES(sizeof(CMEntry)));
	service._clog = (ChunkLog *) service._cen;
	close(fd);

	/* Read Index Table */
	service._db = kcdbnew();
	kcdbopen(service._db, "-", KCOWRITER | KCOCREATE);
	kcdbloadsnap(service._db, DATA_DIR "index");
#if DISABLE_BLOOM == 0
	/* Init Bloom Filter */
	bloom_init(&service._bl, (service._slog->segID + 1048576), 1.0 / 16384);

	for (i = 1; i <= service._slog->segID; i++) {
		if (service._sen[i].ref != -1) {
			bloom_add(&service._bl, service._sen[i].fp, FP_SIZE);
		}
	}
#endif

	ret = pthread_create(&service._tid, NULL, process, NULL );
	return ret;
}

/**
 * Implements BucketService->stop()
 */
static int stop() {
	int ret = pthread_join(service._tid, NULL );
#if DISABLE_BLOOM == 0
	bloom_free(&service._bl);
#endif
	kcdbdumpsnap(service._db, DATA_DIR "index");
	kcdbclose(service._db);
	kcdbdel(service._db);

	munmap(service._sen, MAX_ENTRIES(sizeof(SMEntry)));
	munmap(service._cen, MAX_ENTRIES(sizeof(CMEntry)));
	return ret;
}

/**
 * Implements IndexService->putSegment()
 */
static int putSegment(Segment * seg, uint64_t bucket) {
	memcpy(service._sen[seg->id].fp, seg->fp, FP_SIZE);
	service._sen[seg->id].bucket = bucket;
	service._sen[seg->id].pos = seg->pos;
	service._sen[seg->id].len = seg->clen;
	service._sen[seg->id].cid = seg->cid;
	service._sen[seg->id].chunks = seg->chunks;
	service._sen[seg->id].compressed = seg->compressed;
	service._sen[seg->id].removed = 0;
	memcpy(service._cen + seg->cid, seg->en, seg->chunks * sizeof(CMEntry));
	return 0;
}

/**
 * Implements IndexService->getSegment()
 */
static int getSegment(Segment * seg) {
	memcpy(seg->fp, service._sen[seg->id].fp, FP_SIZE);
	seg->pos = service._sen[seg->id].pos;
	seg->clen = service._sen[seg->id].len;
	seg->cid = service._sen[seg->id].cid;
	seg->chunks = service._sen[seg->id].chunks;
	seg->compressed = service._sen[seg->id].compressed;
	memcpy(seg->en, service._cen + seg->cid, seg->chunks * sizeof(CMEntry));
	return 0;
}


static IndexService service = {
		.start = start,
		.stop = stop,
		.putSegment = putSegment,
		.getSegment = getSegment,
};

IndexService* GetIndexService() {
	return &service;
}
