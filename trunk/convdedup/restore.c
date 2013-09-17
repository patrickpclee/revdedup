/*
 * restore.c
 *
 *  Created on: Jun 20, 2013
 *      Author: chng
 */

#include <convdedup.h>
#include <queue.h>
#include <datatable.h>
#include "minilzo.h"

#define DECOMP_TCNT 6

CMEntry * cen;
BMEntry * ben;

void * prefetch(void * ptr) {
	Queue * q = (Queue *)ptr;
	uint64_t bid, lbid = 0;
	char buf[128];
	while ((bid = (uint64_t)Dequeue(q)) != 0) {
		if (lbid == bid) {
			continue;
		}
		sprintf(buf, DATA_DIR "bucket/%08lx", bid);
		int fd = open(buf, O_RDONLY);
		posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
		close(fd);
		lbid = bid;
	}
	return NULL;
}

typedef struct {
	int ifd;
	int ofd;
	Direct * dir;
	uint64_t cnt;
	volatile uint64_t w_ptr;
	pthread_spinlock_t mutex;
	DataTable * dt;
} DataBuffer;

void * decompress(void * ptr) {
	DataBuffer * db = (DataBuffer *) ptr;
	DataTable * dt = db->dt;
	uint8_t * data;
	uint8_t * cdata = MMAP_MM(MAX_COMPRESSED_SIZE);
	Direct * dir;
	uint64_t w_ptr, size;
	int32_t fd;
	char buf[128];
	while (db->w_ptr < db->cnt) {
		pthread_spin_lock(&db->mutex);
		w_ptr = db->w_ptr++;
		pthread_spin_unlock(&db->mutex);

		dir = &db->dir[w_ptr];
		pthread_mutex_lock(&dt->en[dir->id].mutex);
		if (dt->en[dir->id].cnt == 0) {
			sprintf(buf, DATA_DIR "bucket/%08lx", cen[dir->id].bucket);
			fd = open(buf, O_RDONLY);

			pread(fd, cdata, cen[dir->id].len, cen[dir->id].pos);
			close(fd);
			data = malloc(MAX_CHUNK_SIZE);
			lzo1x_decompress(cdata, cen[dir->id].len, data, &size, NULL);
			dt->en[dir->id].size = size;
			dt->en[dir->id].data = data;
		}
		dt->en[dir->id].cnt++;
		pthread_cond_signal(&dt->en[dir->id].cond);
		pthread_mutex_unlock(&dt->en[dir->id].mutex);
	}

	munmap(cdata, MAX_COMPRESSED_SIZE);
	return NULL;
}

void * send(void * ptr) {
	DataBuffer * db = (DataBuffer *) ptr;
	DataTable * dt = db->dt;
	uint64_t i, j;

	for (i = 0; i < db->cnt; i++) {
		Direct * dir = &db->dir[i];
		pthread_mutex_lock(&dt->en[dir->id].mutex);
		if (dt->en[dir->id].cnt == 0) {
			pthread_cond_wait(&dt->en[dir->id].cond, &dt->en[dir->id].mutex);
		}
		pthread_mutex_unlock(&dt->en[dir->id].mutex);

		assert(
				write(db->ofd, dt->en[dir->id].data, dt->en[dir->id].size)
						== dt->en[dir->id].size);
		pthread_mutex_lock(&dt->en[dir->id].mutex);
		dt->en[dir->id].cnt--;
		if (dt->en[dir->id].cnt == 0) {
			free(dt->en[dir->id].data);
		}
		pthread_mutex_unlock(&dt->en[dir->id].mutex);
	}
	return NULL;
}

int main(int argc, char * argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Usage: %s ID out\n", argv[0]);
		return 0;
	}
	char buf[128];
	int fd;

	uint32_t i;

	/* Retrieve ChunkMeta */
	fd = open(DATA_DIR "clog", O_RDONLY);
	cen = MMAP_FD_RO(fd, MAX_ENTRIES * sizeof(CMEntry));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDONLY);
	ben = MMAP_FD_RO(fd, MAX_ENTRIES * sizeof(BMEntry));
	close(fd);

	/* Retrieve Image data */
	sprintf(buf, DATA_DIR "image/%u", atoi(argv[1]));

	/* Setup Decompress */
	DataBuffer db;
	db.ifd = open(buf, O_RDONLY);
	db.ofd = open(argv[2], O_RDWR | O_CREAT | O_TRUNC, 0644);
	db.cnt = lseek(db.ifd, 0, SEEK_END) / sizeof(Direct);
	db.w_ptr = 0;
	pthread_spin_init(&db.mutex, PTHREAD_PROCESS_SHARED);
	db.dir = MMAP_FD_RO(db.ifd, db.cnt * sizeof(Direct));
	db.dt = dt_create(((ChunkLog *) cen)->chunkID + 1);

	/* Setup Prefetch */
	Queue * pfq = LongQueue();
	pthread_t pft;
	pthread_create(&pft, NULL, prefetch, pfq);

	uint64_t bid, lbid = 0;
	for (i = 0; i < db.cnt; i++) {
		bid = cen[db.dir[i].id].bucket;
		if (lbid == bid) {
			continue;
		}
		lbid = bid;
		Enqueue(pfq, (void *)bid);
	}

	pthread_t did[DECOMP_TCNT];
	for (i = 0; i < DECOMP_TCNT; i++) {
		pthread_create(did + i, NULL, decompress, &db);
	}

	/* Setup Send */
	pthread_t sendt;
	pthread_create(&sendt, NULL, send, &db);

	for (i = 0; i < DECOMP_TCNT; i++) {
		pthread_join(did[i], NULL);
	}

	pthread_join(sendt, NULL);

	pthread_cancel(pft);
	free(pfq);

	munmap(cen, MAX_ENTRIES * sizeof(CMEntry));
	munmap(ben, MAX_ENTRIES * sizeof(BMEntry));
	return 0;
}
