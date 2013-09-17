/*
 * delete.c
 *
 *  Created on: Aug 21, 2013
 *      Author: chng
 */

#include <convdedup.h>
#include <queue.h>
#include <sys/time.h>

typedef struct {
	CMEntry * en;
	uint8_t data[MAX_COMPRESSED_SIZE];
} SimpleChunk;

CMEntry * cen;
BMEntry * ben;

uint8_t padding[BLOCK_SIZE];

static Bucket * NewBucket() {
	char buf[64];
	Bucket * b = malloc(sizeof(Bucket));
	b->id = ++((BucketLog *) ben)->bucketID;
	b->cid = ((ChunkLog *)cen)->chunkID;
	b->chunks = 0;
	b->size = 0;

	sprintf(buf, DATA_DIR "bucket/%08lx", b->id);
	b->fd = open(buf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	assert(b->fd != -1);
	return b;
}

static void SaveBucket(Bucket * b) {
	ssize_t remain = (BLOCK_SIZE - (b->size % BLOCK_SIZE)) % BLOCK_SIZE;
	assert(write(b->fd, padding, remain) == remain);
	close(b->fd);

	ben[b->id].cid = b->cid;
	ben[b->id].chunks = b->chunks;
	ben[b->id].size = b->size + remain;
	ben[b->id].psize = 0;
	ben[b->id].rsize = 0;
	free(b);
}

static Bucket * BucketInsert(Bucket * b, SimpleChunk * ch) {
	if (b == NULL) {
		b = NewBucket();
	}
	if (b->size + ch->en->len > BUCKET_SIZE) {
		SaveBucket(b);
		b = NewBucket();
	}
	if (ch->en - cen < b->cid) {
		b->cid = ch->en - cen;
	}

	ch->en->bucket = b->id;
	ch->en->pos = b->size;

	assert(write(b->fd, ch->data, ch->en->len) == ch->en->len);
	b->chunks++;
	b->size += ch->en->len;

	return b;
}


static void * process(void * ptr) {
	Queue * iq = (Queue *)ptr;
	Bucket * b = NULL;
	SimpleChunk * ch;
	memset(padding, 0, BLOCK_SIZE);
	while ((ch = (SimpleChunk *)Dequeue(iq)) != NULL) {
		b = BucketInsert(b, ch);
		free(ch);
	}
	if (b != NULL) {
		SaveBucket(b);
	}
	return NULL;
}


int main(int argc, char * argv[]) {
	if (argc != 1) {
		fprintf(stderr, "Usage : %s\n", argv[0]);
		return 0;
	}
	char buf[128];
	int fd;
	fd = open(DATA_DIR "clog", O_RDWR);
	cen = MMAP_FD(fd, MAX_ENTRIES * sizeof(CMEntry));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDWR);
	ben = MMAP_FD(fd, MAX_ENTRIES * sizeof(BMEntry));
	close(fd);

	Queue * cq = NewQueue();
	pthread_t wid;
	pthread_create(&wid, NULL, process, cq);

	uint64_t i, j, ptr;
	uint64_t bucketID = ((BucketLog *)ben)->bucketID;
	for (i = 1; i <= bucketID; i++) {
		if (ben[i].rsize == 0) {
			continue;
		}
		sprintf(buf, DATA_DIR "bucket/%08lx", i);
		fd = open(buf, O_RDONLY);
		for (j = 0, ptr = ben[i].cid; j < ben[i].chunks; ptr++) {
			if (cen[ptr].bucket != i) {
				continue;
			}
			if (cen[ptr].ref > 0) {
				SimpleChunk * ch = malloc(sizeof(SimpleChunk));
				ch->en = &cen[ptr];
				pread(fd, ch->data, ch->en->len, ch->en->pos);
				Enqueue(cq, ch);
			}
			j++;
		}
		close(fd);
		unlink(buf);
		memset(ben + i, 0, sizeof(BMEntry));
	}

	Enqueue(cq, NULL);
	pthread_join(wid, NULL);
	free(cq);
	sync();

	munmap(ben, MAX_ENTRIES * sizeof(BMEntry));
	munmap(cen, MAX_ENTRIES * sizeof(CMEntry));

	return 0;
}
