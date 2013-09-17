/*
 * restore.c
 *
 *  Created on: Jun 20, 2013
 *      Author: chng
 */

#include <revdedup.h>
#include <queue.h>
#include <datatable.h>
#include "minilzo.h"

#define DECOMP_TCNT 6

off_t isize;

IMEntry * ien;
SMEntry * sen;
CMEntry * cen;
BMEntry * ben;

uint32_t ins;
uint32_t ver;

void * prefetch(void * ptr) {
	Queue * q = (Queue *) ptr;
	uint64_t sid;
	char buf[128];
	while ((sid = (uint64_t) Dequeue(q)) != 0) {
		sprintf(buf, DATA_DIR "bucket/%08lx", sen[sid].bucket);
		int fd = open(buf, O_RDONLY);
		posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
		close(fd);
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
	uint8_t * temp = MMAP_MM(MAX_SEG_SIZE);
	Direct * dir;
	uint64_t w_ptr, size;
	int32_t fd, i;
	char buf[128];
	while (1) {
		pthread_spin_lock(&db->mutex);
		w_ptr = db->w_ptr++;
		pthread_spin_unlock(&db->mutex);
		if (w_ptr >= db->cnt) {
			break;
		}
		dir = &db->dir[w_ptr];
		pthread_mutex_lock(&dt->en[dir->id].mutex);
		if (dt->en[dir->id].bldg != 2) {
			pthread_mutex_unlock(&dt->en[dir->id].mutex);
			continue;
		}

		dt->en[dir->id].bldg = 1;
		pthread_mutex_unlock(&dt->en[dir->id].mutex);
		sprintf(buf, DATA_DIR "bucket/%08lx", sen[dir->id].bucket);
		fd = open(buf, O_RDONLY);
		pread(fd, cdata, sen[dir->id].len, sen[dir->id].pos);
		close(fd);

		lzo1x_decompress(cdata, sen[dir->id].len, temp, &size, NULL);
		size = 0;
		for (i = 0; i < sen[dir->id].chunks; i++) {
			if (cen[sen[dir->id].cid + i].len == 0) {
				memset(dt->en[dir->id].data + size, 0, ZERO_SIZE);
				size += ZERO_SIZE;
			} else {
				memcpy(dt->en[dir->id].data + size,
						temp + cen[sen[dir->id].cid + i].pos,
						cen[sen[dir->id].cid + i].len);
				size += cen[sen[dir->id].cid + i].len;
			}
		}
		dt->en[dir->id].size = size;

		pthread_mutex_lock(&dt->en[dir->id].mutex);
		dt->en[dir->id].bldg = 0;
		pthread_mutex_unlock(&dt->en[dir->id].mutex);
		pthread_cond_signal(&dt->en[dir->id].cond);
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
		if (dt->en[dir->id].bldg) {
			pthread_cond_wait(&dt->en[dir->id].cond, &dt->en[dir->id].mutex);
		}
		pthread_mutex_unlock(&dt->en[dir->id].mutex);

		assert(write(db->ofd, dt->en[dir->id].data, dt->en[dir->id].size) == dt->en[dir->id].size);

		if (--dt->en[dir->id].cnt == 0) {
			munmap(dt->en[dir->id].data, MAX_SEG_SIZE);
		}
	}
	return NULL;
}

int main(int argc, char * argv[]) {
	if (argc != 4) {
		fprintf(stderr, "Usage: %s instance version file\n", argv[0]);
		return 0;
	}
	char buf[128];
	ins = atoi(argv[1]);
	ver = atoi(argv[2]);
	sprintf(buf, DATA_DIR "image/i%u-%u", ins, ver);
	if (access(buf, F_OK) == 0) {
		fprintf(stderr, "This version is get revdeduped, try using restoreo\n");
		return 0;
	}

	uint32_t i;
	int fd;
	/* Retrieve Metadata */
	fd = open(DATA_DIR "ilog", O_RDONLY);
	ien = MMAP_FD_RO(fd, INST_MAX * sizeof(IMEntry));
	close(fd);

	fd = open(DATA_DIR "slog", O_RDONLY);
	sen = MMAP_FD_RO(fd, MAX_ENTRIES * sizeof(SMEntry));
	close(fd);

	fd = open(DATA_DIR "clog", O_RDONLY);
	cen = MMAP_FD_RO(fd, MAX_ENTRIES * sizeof(CMEntry));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDONLY);
	ben = MMAP_FD_RO(fd, MAX_ENTRIES * sizeof(BMEntry));
	close(fd);

	/* Retrieve Image data */
	sprintf(buf, DATA_DIR "image/%u-%u", ins, ver);

	/* Setup Decompress */
	DataBuffer db;
	db.ifd = open(buf, O_RDONLY);
	db.ofd = creat(argv[3], 0644);
	db.cnt = lseek(db.ifd, 0, SEEK_END) / sizeof(Direct);
	db.w_ptr = 0;
	pthread_spin_init(&db.mutex, PTHREAD_PROCESS_SHARED);
	db.dir = MMAP_FD_RO(db.ifd, db.cnt * sizeof(Direct));
	db.dt = dt_create(((SegmentLog *)sen)->segID + 1);

	/* Setup Prefetch */
	Queue * pfq = SuperQueue();
	pthread_t pft;
	pthread_create(&pft, NULL, prefetch, pfq);

	uint64_t sid, lsid = 0;
	for (i = 0; i < db.cnt; i++) {
		if (db.dt->en[db.dir[i].id].cnt == 0) {
			db.dt->en[db.dir[i].id].data = MMAP_MM(MAX_SEG_SIZE);
			db.dt->en[db.dir[i].id].bldg = 2;
		}
		db.dt->en[db.dir[i].id].cnt++;
	}

	for (i = 0; i < db.cnt; i++) {
		sid = db.dir[i].id;
		if (sid == lsid) {
			continue;
		}
		Enqueue(pfq, (void *) sid);
		lsid = sid;
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


	munmap(ien, INST_MAX * sizeof(IMEntry));
	munmap(sen, MAX_ENTRIES * sizeof(SMEntry));
	munmap(cen, MAX_ENTRIES * sizeof(CMEntry));
	munmap(ben, MAX_ENTRIES * sizeof(BMEntry));
	return 0;
}
