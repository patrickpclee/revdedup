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
	uint32_t ver;
	uint32_t lver;
	volatile uint64_t end;
	uint64_t chunks;
	Indirect * id;
	ImageData * idata;
} IndirectMap;

typedef struct {
	uint32_t ver;
	int ofd;
	volatile uint64_t cur;
	pthread_spinlock_t lock;
	IndirectMap * map;
	Queue * pfq;
} DataInfo;

IMEntry * ien;
SMEntry * sen;
CMEntry * cen;
BMEntry * ben;

DataInfo dinfo;
DataTable * dt;
Queue * dq;

void * prefetch(void * ptr) {
	Queue * q = (Queue *) ptr;
	uint64_t sid, lsid = 0, bid = 0;
	uint32_t pos, len;
	char buf[128];
	while ((sid = (uint64_t) Dequeue(q)) != 0) {
		if (lsid == sid) {
			continue;
		}
		lsid = sid;
		if (bid == 0) {
			bid = sen[sid].bucket;
			pos = sen[sid].pos;
			len = sen[sid].len;
			continue;
		}
		if (sen[sid].bucket == bid && pos + len == sen[sid].pos) {
			len += sen[sid].len;
			continue;
		}

		sprintf(buf, DATA_DIR "bucket/%08lx", bid);
		int fd = open(buf, O_RDONLY);
		posix_fadvise(fd, pos, len, POSIX_FADV_WILLNEED);
		close(fd);

		bid = sen[sid].bucket;
		pos = sen[sid].pos;
		len = sen[sid].len;
	}
	sprintf(buf, DATA_DIR "bucket/%08lx", bid);
	int fd = open(buf, O_RDONLY);
	posix_fadvise(fd, pos, len, POSIX_FADV_WILLNEED);
	close(fd);

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
	idata->d = MMAP_FD_RO(fd, idata->dsize);
	close(fd);

	idata->idsize = idata->dsize / sizeof(Direct) * MAX_SEG_CHUNKS * sizeof(Indirect);
	idata->id = MMAP_MM(idata->idsize);

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
		for (j = 0; j < ien.len; j++) {
			uint64_t p = index + i * MAX_SEG_CHUNKS;
			idata->id[p].ptr = ien.ptr & (UNIQ - 1);
			idata->id[p].pos = ien.pos + j;
			idata->id[p].ver = idata->ver + !(ien.ptr & UNIQ);
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
	IndirectMap * map = dinfo.map;
	Queue * pfq = dinfo.pfq;
	uint64_t i, j, k;
	uint64_t sid;
	uint32_t offset, len;
	map->end = 0;

	for (i = 0; i < map->idata[0].dsize / sizeof(Direct); i++) {
		for (j = 0; j < sen[map->idata[0].d[i].id].chunks; j++) {
			uint64_t p = map->end;
			map->id[p] = map->idata[0].id[i * MAX_SEG_CHUNKS + j];
			for (k = map->ver + 1; k < map->lver; k++) {
				if (map->id[map->end].ver < k) {
					break;
				}
				uint64_t q = map->id[p].ptr * MAX_SEG_CHUNKS
						+ map->id[p].pos;
				map->id[p] = map->idata[k - map->ver].id[q];
			}
			map->end++;

			/* Setup DataTable */
			sid = map->idata[map->id[p].ver - map->ver].d[map->id[p].ptr].id;
			if (cen[sen[sid].cid + map->id[p].pos].len == 0) {
				continue;
			}
			if (++dt->en[sid].cnt == 1) {
				pthread_spin_init(&dt->en[sid].lock, PTHREAD_PROCESS_SHARED);
				pthread_mutex_lock(&dt->en[sid].mutex);
				Enqueue(pfq, (void *)sid);
			}
		}
	}
	Enqueue(pfq, NULL);
	return NULL;
}

Indirect * getIndirect(IndirectMap * map, uint64_t cur) {
	while (cur >= map->end);
	return &map->id[cur];
}

void * decompress(void * ptr) {
	IndirectMap * map = dinfo.map;
	uint8_t * cdata = MMAP_MM(MAX_SEG_SIZE);
	SMEntry * en;
	DataEntry * den;
	uint64_t size, cur, seg;
	uint32_t fd;
	char buf[128];


	while (1) {
		pthread_spin_lock(&dinfo.lock);
		cur = dinfo.cur++;
		pthread_spin_unlock(&dinfo.lock);
		if (unlikely(cur >= map->chunks)) {
			break;
		}
		Indirect * in = getIndirect(map, cur);
		seg = map->idata[in->ver - dinfo.ver].d[in->ptr].id;
		if (pthread_spin_trylock(&dt->en[seg].lock)) {
			continue;
		}
		en = &sen[seg];
		den = &dt->en[seg];
		den->data = Dequeue(dq);
		sprintf(buf, DATA_DIR "bucket/%08lx", en->bucket);
		fd = open(buf, O_RDONLY);
#ifdef DISABLE_COMPRESSION
		assert(pread(fd, den->data, en->len, en->pos) == en->len);
#else
		if (sen->compressed) {
			assert(pread(fd, cdata, en->len, en->pos) == en->len);
			lzo1x_decompress(cdata, en->len, den->data, &size, NULL);
			den->size = size;
		} else {
			assert(pread(fd, den->data, en->len, en->pos) == en->len);
			den->size = en->len;
		}
#endif
		close(fd);
		pthread_mutex_unlock(&dt->en[seg].mutex);
	}

	munmap(cdata, MAX_SEG_SIZE);
	return NULL;
}

void * send(void * ptr) {
	IndirectMap * map = dinfo.map;
	void * zdata = MMAP_MM(ZERO_SIZE);
	uint64_t i, j;

	for (i = 0; i < map->chunks; i++) {
		Indirect * in = getIndirect(dinfo.map, i);
		uint64_t seg = map->idata[in->ver - dinfo.ver].d[in->ptr].id;
		uint32_t pos = cen[sen[seg].cid + in->pos].pos;
		uint32_t len = cen[sen[seg].cid + in->pos].len;
		if (len == 0) {
			assert(write(dinfo.ofd, zdata, ZERO_SIZE) == ZERO_SIZE);
			continue;
		}
		pthread_mutex_lock(&dt->en[seg].mutex);
		pthread_mutex_unlock(&dt->en[seg].mutex);
		assert(write(dinfo.ofd, dt->en[seg].data + pos, len) == len);
		if (--dt->en[seg].cnt == 0) {
			Enqueue(dq, dt->en[seg].data);
		}
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
	int32_t fd;
	uint32_t inst = atoi(argv[1]);
	uint32_t ver = atoi(argv[2]);

	sprintf(buf, DATA_DIR "image/i%u-%u", inst, ver);
	if (access(buf, F_OK) == -1) {
		fprintf(stderr, "This version is still new, try using restore\n");
		return 0;
	}

	uint32_t lver = 0;
	uint32_t i, j, k;

	/* Retrieve Metadata */
	fd = open(DATA_DIR "ilog", O_RDONLY);
	ien = MMAP_FD_RO(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	fd = open(DATA_DIR "slog", O_RDONLY);
	sen = MMAP_FD_RO(fd, MAX_ENTRIES(sizeof(SMEntry)));
	close(fd);

	fd = open(DATA_DIR "clog", O_RDONLY);
	cen = MMAP_FD_RO(fd, MAX_ENTRIES(sizeof(CMEntry)));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDONLY);
	ben = MMAP_FD_RO(fd, MAX_ENTRIES(sizeof(BMEntry)));
	close(fd);

	lver = ien[inst].versions - ien[inst].recent;

	/* Building Indirect Map */
	ImageData * idata = malloc((lver - ver + 1) * sizeof(ImageData));
	for (i = 0; i < lver - ver + 1; i++) {
		idata[i].inst = inst;
		idata[i].ver = ver + i;
		pthread_create(&idata[i].tid, NULL, buildIndirect, &idata[i]);
	}

	/* Merge Indirect maps to single map */
	IndirectMap map;

	map.ver = ver;
	map.lver = lver;
	map.end = 0;
	map.chunks = 0;
	map.idata = idata;

	/* Initialize DataInfo */
	dinfo.ver = ver;
	dinfo.ofd = creat(argv[3], 0644);
	dinfo.cur = 0;
	dinfo.map = &map;
	pthread_spin_init(&dinfo.lock, PTHREAD_PROCESS_SHARED);

	dt = NewDataTable(((SegmentLog *)sen)->segID + 1);

	void * data = MMAP_MM(LONGQUEUE_LENGTH * MAX_SEG_SIZE);
	dq = LongQueue();
	for (i = 0; i < LONGQUEUE_LENGTH; i++) {
		Enqueue(dq, data + i * MAX_SEG_SIZE);
	}

	for (i = 0; i < lver - ver + 1; i++) {
		pthread_join(idata[i].tid, NULL);
	}

	/* Setup Prefetch */
	dinfo.pfq = SuperQueue();
	pthread_t pft;
	pthread_create(&pft, NULL, prefetch, dinfo.pfq);

	map.id = MMAP_MM(idata[0].idsize);
	for (i = 0; i < idata[0].dsize / sizeof(Direct); i++) {
		map.chunks += sen[idata[0].d[i].id].chunks;
	}

	pthread_t mgt;
	pthread_create(&mgt, NULL, mergeIndirects, NULL);
	pthread_join(mgt, NULL);

	/** setup decompress and send */
	pthread_t dct[DPS_CNT];
	pthread_t sdt;
	for (i = 0; i < DPS_CNT; i++) {
		pthread_create(dct + i, NULL, decompress, &dinfo);
	}
	pthread_create(&sdt, NULL, send, &dinfo);

	/** wait for the outstanding threads */
	for (i = 0; i < DPS_CNT; i++) {
		pthread_join(dct[i], NULL);
	}
	pthread_join(sdt, NULL);
	pthread_cancel(pft);

	DelQueue(dinfo.pfq);
	DelQueue(dq);
	munmap(data, LONGQUEUE_LENGTH * MAX_SEG_SIZE);

	DelDataTable(dt);

	munmap(ien, INST_MAX(sizeof(IMEntry)));
	munmap(sen, MAX_ENTRIES(sizeof(SMEntry)));
	munmap(cen, MAX_ENTRIES(sizeof(CMEntry)));
	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));

	close(dinfo.ofd);
	return 0;
}
