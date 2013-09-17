/*
 * restoreo.c
 *
 *  Created on: Jun 20, 2013
 *      Author: chng
 */

#include <revdedup.h>
#include <queue.h>
#include <datatable.h>
#include "minilzo.h"

#define DECOMP_TCNT 16

typedef struct {
	pthread_t tid;
	uint32_t inst;
	uint32_t ver;
	uint64_t dsize;
	uint64_t idsize;
	Direct * d;
	Indirect * id;
} ImageData;

typedef struct {
	pthread_t tid;
	uint32_t ver;
	uint32_t lver;
	volatile uint64_t end;
	uint64_t chunks;
	Indirect * id;
	ImageData * idata;
	Queue * pfq;
} IndirectMap;

off_t isize;

IMEntry * ien;
SMEntry * sen;
CMEntry * cen;
BMEntry * ben;

void * prefetch(void * ptr) {
	Queue * q = (Queue *)ptr;
	uint64_t sid, lsid = 0;
	char buf[128];
	while ((sid = (uint64_t)Dequeue(q)) != 0) {
		if (lsid == sid) {
			continue;
		}
		sprintf(buf, DATA_DIR "bucket/%08lx", sen[sid].bucket);
		int fd = open(buf, O_RDONLY);
		posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
		close(fd);
		lsid = sid;
	}
	return NULL;
}

void * buildIndirect(void * ptr) {
	ImageData * idata = (ImageData * )ptr;
	char buf[256];
	uint32_t i = 0, j;

	sprintf(buf, DATA_DIR "image/%u-%u", idata->inst, idata->ver);
	int fd = open(buf, O_RDONLY);
	assert(fd != -1);
	idata->dsize = lseek(fd, 0, SEEK_END);
	posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
	idata->d = mmap(0, idata->dsize, PROT_READ, MAP_SHARED, fd, 0);
	close(fd);

	idata->idsize = idata->dsize / sizeof(Direct) * MAX_SEG_CHUNKS * sizeof(Indirect);
	idata->id = mmap(0, idata->idsize, 0x3, 0x2 | MAP_ANONYMOUS, -1, 0);

	sprintf(buf, DATA_DIR "image/i%u-%u", idata->inst, idata->ver);
	fd = open(buf, O_RDONLY);
	if (fd == -1) {
		return NULL ;
	}
	posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);

	uint64_t index = 0;
	while (i < idata->dsize / sizeof(Direct)
			&& index < sen[idata->d[i].id].chunks) {
		Indirect ien;
		assert(read(fd, &ien, sizeof(ien)) == sizeof(ien));
		for (j = 0; j < ien.count; j++) {
			uint64_t p = index + i * MAX_SEG_CHUNKS;
			idata->id[p].ptr = ien.ptr & (NODEDUP - 1);
			idata->id[p].offset = ien.offset + j;
			idata->id[p].version = idata->ver + !(ien.ptr & NODEDUP);
			index++;
			if (index == sen[idata->d[i].id].chunks) {
				i++;
				index = 0;
			}
		}
	}
	close(fd);
	return NULL;
}

void * mergeIndirects(void * ptr) {
	IndirectMap * map = (IndirectMap *) ptr;
	uint64_t i, j, k;
	uint64_t sid;
	uint8_t * lsids = malloc(((SegmentLog *)sen)->segID + 1);
	map->end = 0;

	for (i = 0; i < map->idata[0].dsize / sizeof(Direct); i++) {
		for (j = 0; j < sen[map->idata[0].d[i].id].chunks; j++) {
			uint64_t p = map->end;
			map->id[p] = map->idata[0].id[i * MAX_SEG_CHUNKS + j];
			for (k = map->ver + 1; k < map->lver; k++) {
				if (map->id[map->end].version < k) {
					break;
				}
				uint64_t q = map->id[p].ptr * MAX_SEG_CHUNKS
						+ map->id[p].offset;
				map->id[p] = map->idata[k - map->ver].id[q];
			}
			map->end++;

			sid = map->idata[map->id[p].version - map->ver].d[map->id[p].ptr].id;
			if (lsids[sid] != 1) {
				Enqueue(map->pfq, (void *) sid);
				lsids[sid] = 1;
			}
		}
	}
	free(lsids);
	return NULL;
}

Indirect * getIndirect(IndirectMap * map, uint64_t cur) {
	while (cur >= map->end);
	return &map->id[cur];
}


typedef struct {
	uint32_t ver;
	int ofd;
	volatile uint64_t w_ptr;
	pthread_spinlock_t lock;
	DataTable * dt;
	IndirectMap * map;
} DataBuffer;

void * decompress(void * ptr) {
	DataBuffer * db = (DataBuffer *) ptr;
	DataTable * dt = db->dt;
	IndirectMap * map = db->map;
	uint8_t * data;
	uint8_t * cdata = MMAP_MM(MAX_COMPRESSED_SIZE);
	uint64_t w_ptr, size;
	int32_t fd;
	char buf[128];
	while (1) {
		pthread_spin_lock(&db->lock);
		w_ptr = db->w_ptr++;
		pthread_spin_unlock(&db->lock);
		if (w_ptr >= map->chunks) {
			break;
		}
		Indirect * en = getIndirect(db->map, w_ptr);
		uint64_t seg = map->idata[en->version - db->ver].d[en->ptr].id;
		pthread_mutex_lock(&dt->en[seg].mutex);
		if (dt->en[seg].cnt == 0 && dt->en[seg].bldg == 0) {
			dt->en[seg].bldg = 1;
			pthread_mutex_unlock(&dt->en[seg].mutex);

			sprintf(buf, DATA_DIR "bucket/%08lx", sen[seg].bucket);
			fd = open(buf, O_RDONLY);
			pread(fd, cdata, sen[seg].len, sen[seg].pos);
			close(fd);
			data = malloc(MAX_SEG_SIZE);
			lzo1x_decompress(cdata, sen[seg].len, data, &size, NULL);
			dt->en[seg].data = data;

			pthread_mutex_lock(&dt->en[seg].mutex);
			dt->en[seg].bldg = 0;
			pthread_cond_signal(&dt->en[seg].cond);
		}
		dt->en[seg].cnt++;
		pthread_mutex_unlock(&dt->en[seg].mutex);
	}
	munmap(cdata, MAX_COMPRESSED_SIZE);
	return NULL;
}

void * send(void * ptr) {
	DataBuffer * db = (DataBuffer *) ptr;
	IndirectMap * map = db->map;
	DataTable * dt = db->dt;
	void * zdata = MMAP_MM(ZERO_SIZE);
	uint64_t i, j;

	for (i = 0; i < map->chunks; i++) {
		Indirect * en = getIndirect(db->map, i);
		uint64_t seg = map->idata[en->version - db->ver].d[en->ptr].id;
		uint32_t pos = cen[sen[seg].cid + en->offset].pos;
		uint32_t len = cen[sen[seg].cid + en->offset].len;
		if (len == 0) {
			assert(write(db->ofd, zdata, ZERO_SIZE) == ZERO_SIZE);
		} else {
			// Optimize by removing useless lock and unlock
			if (unlikely(dt->en[seg].cnt == 0 || dt->en[seg].bldg == 1)) {
				pthread_mutex_lock(&dt->en[seg].mutex);
				if (dt->en[seg].cnt == 0 || dt->en[seg].bldg == 1) {
					pthread_cond_wait(&dt->en[seg].cond, &dt->en[seg].mutex);
				}
				pthread_mutex_unlock(&dt->en[seg].mutex);
			}
			assert(write(db->ofd, dt->en[seg].data + pos, len) == len);
		}
		pthread_mutex_lock(&dt->en[seg].mutex);
		if (--dt->en[seg].cnt == 0) {
			free(dt->en[seg].data);
		}
		pthread_mutex_unlock(&dt->en[seg].mutex);
	}
	munmap(zdata, ZERO_SIZE);
	return NULL;
}

int main(int argc, char * argv[]) {
	if (argc != 4) {
		fprintf(stderr, "Usage: %s instance version file\n", argv[0]);
		return 0;
	}
	char buf[128];
	int fd;
	uint32_t inst = atoi(argv[1]);
	uint32_t ver = atoi(argv[2]);

	sprintf(buf, DATA_DIR "image/i%u-%u", inst, ver);
	if (access(buf, F_OK) == -1) {
		fprintf(stderr, "This version is still new, try using restore\n");
		return 0;
	}

	uint32_t lver = 0;
	uint32_t i, j, k;

	/* Setup Prefetch */
	Queue * q = NewQueue();
	pthread_t tid;
	pthread_create(&tid, NULL, prefetch, q);

	/* Retrieve Metadata */
	fd = open(DATA_DIR "ilog", O_RDONLY);
	isize = lseek(fd, 0, SEEK_END);
	ien = mmap(0, isize, 0x1, 0x2, fd, 0);
	close(fd);

	fd = open(DATA_DIR "slog", O_RDONLY);
	sen = mmap(0, MAX_ENTRIES * sizeof(SMEntry), 0x1, 0x2, fd, 0);
	close(fd);

	fd = open(DATA_DIR "clog", O_RDONLY);
	cen = mmap(0, MAX_ENTRIES * sizeof(CMEntry), 0x1, 0x2, fd, 0);
	close(fd);

	fd = open(DATA_DIR "blog", O_RDONLY);
	ben = mmap(0, MAX_ENTRIES * sizeof(BMEntry), 0x1, 0x2, fd, 0);
	close(fd);

	lver = ien[inst].versions - ien[inst].recent;

	/* Building Indirect Map */
	ImageData * idata = malloc((lver - ver + 1) * sizeof(ImageData));
	for (i = 0; i < lver - ver + 1; i++) {
		idata[i].inst = inst;
		idata[i].ver = ver + i;
		pthread_create(&idata[i].tid, NULL, buildIndirect, &idata[i]);
	}

	for (i = 0; i < lver - ver + 1; i++) {
		pthread_join(idata[i].tid, NULL);
	}


	/* Merge Indirect maps to single map */
	IndirectMap map;
	map.ver = ver;
	map.lver = lver;
	map.end = 0;
	map.chunks = 0;
	map.idata = idata;
	map.id = MMAP_MM(idata[0].idsize);
	map.pfq = NewQueue();

	/* Setup Prefetch */
	pthread_t pft;
	pthread_create(&pft, NULL, prefetch, map.pfq);

	for (i = 0; i < idata[0].dsize / sizeof(Direct); i++) {
		map.chunks += sen[idata[0].d[i].id].chunks;
	}
	pthread_create(&map.tid, NULL, mergeIndirects, &map);

	/* Initialize DataBuffer */
	DataBuffer db;
	db.ver = ver;
	db.ofd = open(argv[3], O_RDWR | O_CREAT | O_TRUNC, 0644);
	db.w_ptr = 0;
	pthread_spin_init(&db.lock, PTHREAD_PROCESS_SHARED);
	db.dt = dt_create(((SegmentLog *)sen)->segID + 1);
	db.map = &map;

	/* Setup Decompress */
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
	pthread_join(map.tid, NULL);
	pthread_cancel(pft);

	munmap(ien, isize);
	munmap(sen, MAX_ENTRIES * sizeof(SMEntry));
	munmap(cen, MAX_ENTRIES * sizeof(CMEntry));
	munmap(ben, MAX_ENTRIES * sizeof(BMEntry));

	return 0;
}
