/*
 * revmap.c
 *
 *  Created on: Jun 17, 2013
 *      Author: chng
 */

#include <kclangc.h>
#include <search.h>
#include "revmap.h"


#define CH(x) ((char *)x)
#define SZ_U	sizeof(uint64_t)
#define SZ_I	sizeof(Indirect)

static RevMapService service;

static void * process(void * ptr) {
	uint64_t start = (uint64_t)ptr;
	uint32_t cur;
	int fd;
	char buf[128];
	Direct * idir, * odir;
	uint64_t isize, osize;
	uint64_t icnt, ocnt;
	uint32_t i, j, p;
	size_t sz, sz2;

	for (cur = start; cur < service._ins; cur += REV_CNT) {
		KCMAP * smap = kcmapnew(131072);
		KCMAP * cmap = kcmapnew(131072);
		KCMAP * imap = kcmapnew(131072);

		sprintf(buf, DATA_DIR "image/%u-%u", cur, service._ver + 1);
		fd = open(buf, O_RDONLY);
		isize = lseek(fd, 0, SEEK_END);
		idir = MMAP_FD_RO(fd, isize);
		icnt = isize / sizeof(Direct);
		close(fd);
		sprintf(buf, DATA_DIR "image/%u-%u", cur, service._ver);
		fd = open(buf, O_RDONLY);
		osize = lseek(fd, 0, SEEK_END);
		odir = MMAP_FD_RO(fd, osize);
		ocnt = osize / sizeof(Direct);
		close(fd);

		for (i = 0; i < icnt; i++) {
			SMEntry * sen = &service._sen[idir[i].id];
			Indirect sin = { .ptr = i, .pos = 0, .len = sen->chunks };
			kcmapadd(smap, CH(sen->fp), FP_SIZE, CH(&sin), SZ_I);

			for (j = 0; j < service._sen[idir[i].id].chunks; j++) {
				CMEntry * cen = &service._cen[service._sen[idir[i].id].cid + j];
#ifndef ADD_ZERO_FP
				if (!memcmp(cen->fp, ZERO_FP, FP_SIZE)) {
					continue;
				}
#endif
				Indirect cin = { .ptr = i, .pos = j, .len = 1 };
				kcmapadd(cmap, CH(cen->fp), FP_SIZE, CH(&cin), SZ_I);
			}
		}

		for (i = 0; i < ocnt; i++) {
			uint64_t bpos = htobe64(odir[i].index);
			// Segment Deduplication
			SMEntry * sen = &service._sen[odir[i].id];
			const char * sin = kcmapget(smap, CH(sen->fp), FP_SIZE, &sz);
			if (sin != NULL) {
				kcmapadd(imap, CH(&bpos), SZ_U, CH(sin), SZ_I);
				continue;
			}

			// Chunk Deduplication
			for (j = 0; j < service._sen[odir[i].id].chunks; j++) {
				CMEntry * cen = &service._cen[service._sen[odir[i].id].cid + j];
				bpos = htobe64(odir[i].index + j);	// Cannot use cen->pos, chunks may have 0 length
				const char * cin = kcmapget(cmap, CH(cen->fp), FP_SIZE, &sz);
				if (cin != NULL) {
					kcmapadd(imap, CH(&bpos), SZ_U, CH(cin), SZ_I);
				} else {
					Indirect ncin = { .ptr = UNIQ + i, .pos = j, .len = 1 };
					kcmapadd(imap, CH(&bpos), SZ_U, CH(&ncin), SZ_I);
					pthread_spin_lock(&service._rlock);
					cen->ref++;
					pthread_spin_unlock(&service._rlock);
				}
			}
		}

		// Generate Indirect from B-tree
		sprintf(buf, DATA_DIR "image/i%u-%u", cur, service._ver);
		fd = creat(buf, 0644);

		KCMAPSORT * ismap = kcmapsorter(imap);
		const char * bptr;
		Indirect * in;
		Indirect prev = { .ptr = 0, .pos = 0, .len = 0 };
		while ((bptr = kcmapsortget(ismap, &sz, (const char **) &in, &sz2)) != NULL) {
			// Should merge with the neighbour entries
			if (in->ptr == prev.ptr && in->pos == prev.pos + prev.len) {
				prev.len += in->len;
			} else {
				if (prev.len) {
					assert(write(fd, &prev, SZ_I) == SZ_I);
				}
				prev = *in;
			}
			kcmapsortstep(ismap);
		}
		assert(write(fd, &prev, SZ_I) == SZ_I);
		kcmapsortdel(ismap);
		close(fd);

		kcmapdel(smap);
		kcmapdel(cmap);
		kcmapdel(imap);

		munmap(idir, isize);
		munmap(odir, osize);
	}

	return NULL ;
}

static int start(SMEntry * sen, CMEntry * cen, uint32_t instances, uint32_t version) {
	uint64_t i;
	service._sen = sen;
	service._slog = (SegmentLog *)sen;
	service._cen = cen;
	service._clog = (ChunkLog *)cen;
	service._ins = instances;
	service._ver = version;
	pthread_spin_init(&service._rlock, PTHREAD_PROCESS_SHARED);
	for (i = 0; i < REV_CNT; i++) {
		pthread_create(service._tid + i, NULL, process, (void *)i);
	}
	return 0;
}

static int stop() {
	int i, ret;
	for (i = 0; i < REV_CNT; i++) {
		ret = pthread_join(service._tid[i], NULL );
	}
	return ret;
}

static RevMapService service = {
		.start = start,
		.stop = stop
};


RevMapService * GetRevMapService() {
	return &service;
}
