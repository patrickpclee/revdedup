/*
 * index.c
 *
 *  Created on: May 29, 2013
 *      Author: chng
 */

#include "index.h"
#include "fingerprint.h"

static IndexService service;

static void * process(void * ptr) {
	Chunk * ch;
	uint64_t size;
	while ((ch = (Chunk *) Dequeue(service._iq)) != NULL) {
		if (bloom_add(&service._bl, ch->fp, FP_SIZE)) {
			void * idptr = kcdbget(service._db, (char *)ch->fp, FP_SIZE, &size);
			uint64_t id = idptr ? *(uint64_t *) idptr : 0;
			if (id == 0) {
				ch->id = ++service._log->chunkID;
				ch->unique = 1;
				kcdbadd(service._db, (char *)ch->fp, FP_SIZE, (char *)&ch->id, sizeof(uint64_t));
			} else {
				ch->id = id;
				ch->unique = 0;
			}
		} else {
			ch->id = ++service._log->chunkID;
			ch->unique = 1;
			kcdbadd(service._db, (char *)ch->fp, FP_SIZE, (char *)&ch->id, sizeof(uint64_t));
		}
		service._en[ch->id].ref++;
		Enqueue(service._oq, ch);
	}
	Enqueue(service._oq, NULL);
	return NULL;
}


static int start(Queue * iq, Queue * oq) {
	int ret, fd, i;
	service._iq = iq;
	service._oq = oq;

	/* Read Chunk Metadata */
	fd = open(DATA_DIR "clog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES * sizeof(CMEntry)));
	service._en = MMAP_FD(fd, MAX_ENTRIES * sizeof(CMEntry));
	service._log = (ChunkLog *) service._en;
	close(fd);

	service._db = kcdbnew();
	kcdbopen(service._db, "-", KCOWRITER | KCOCREATE);
	kcdbloadsnap(service._db, DATA_DIR "index");

	/* Init Bloom Filter */
	bloom_init(&service._bl, (service._log->chunkID + 4194304), 1.0 / 16384);
	for (i = 1; i <= service._log->chunkID; i++) {
		if (service._en[i].ref > 0) {
			bloom_add(&service._bl, service._en[i].fp, FP_SIZE);
		}
	}

	ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

static int stop() {
	int ret = pthread_join(service._tid, NULL );
	bloom_free(&service._bl);

	kcdbdumpsnap(service._db, DATA_DIR "index");
	kcdbdel(service._db);
	munmap(service._en, MAX_ENTRIES * sizeof(CMEntry));
	return ret;
}

static int setChunk(Chunk * ch, uint64_t bucket) {
	service._en[ch->id].bucket = bucket;
	memcpy(service._en[ch->id].fp, ch->fp, FP_SIZE);
	service._en[ch->id].pos = ch->pos;
	service._en[ch->id].len = ch->clen;
	return 0;
}

static IndexService service = {
		.start = start,
		.stop = stop,
		.setChunk = setChunk,
};

IndexService* GetIndexService() {
	return &service;
}
