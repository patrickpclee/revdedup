/*
 * revmap.c
 *
 *  Created on: Jun 17, 2013
 *      Author: chng
 */

#include "revmap.h"
#include "hashtable.h"
#include "btree.h"

static RevMapService service;

static void * process(void * ptr) {
	uint64_t start = (uint64_t)ptr;
	uint32_t i, j, k, p;
	char buf[64];
	Direct en;
	for (i = start; i < service._ins; i += REV_TCNT) {
		sprintf(buf, DATA_DIR "image/t%u-%u", i, service._ver);
		BtDb * indb = bt_open(buf, BT_rw, 12, 4096, 10);

		sprintf(buf, DATA_DIR "image/%u-%u", i, service._ver + 1);
		int fd = open(buf, O_RDONLY);
		uint64_t size = lseek(fd, 0, SEEK_END);
		uint32_t count = size / sizeof(Direct);
		Direct * dir = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
		close(fd);

		HTable * seght = HT_CREATE(count * 8, service._sen, SMEntry, fp);
		HTable * chkht = HT_CREATE(count * 8 * MAX_SEG_CHUNKS, service._cen, CMEntry, fp);
		for (j = 0; j < count; j++) {
			HTInsert(seght, dir[j].id, j, 0);
			for (k = 0; k < service._sen[dir[j].id].chunks; k++) {
				uint64_t cid = service._sen[dir[j].id].cid + k;
				if (!memcmp(service._cen[cid].fp, ZERO_FP, FP_SIZE)) {
					continue;
				}
				HTInsert(chkht, cid, j, k);
			}
		}

		sprintf(buf, DATA_DIR "image/%u-%u", i, service._ver);
		fd = open(buf, O_RDONLY);
		for (p = 0; ; p++) {
			if (read(fd, &en, sizeof(Direct)) == 0) {
				break;
			}
			uint64_t bpos = htobe64(en.pos);
			// Segment Deduplication
			HEntry * hen = HTSearch(seght, service._sen[en.id].fp, FP_SIZE);
			if (hen != NULL ) {
				bt_insertkey(indb, RAWPTR(bpos), 8, 0, hen->d1,
						service._sen[hen->index].chunks);
				continue;
			}

			// Chunk Deduplication
			for (j = 0; j < service._sen[en.id].chunks; j++) {
				CMEntry * cen = &service._cen[service._sen[en.id].cid + j];
				bpos = htobe64(en.pos + j); // Cannot use cen->pos, chunks have 0 length
				hen = HTSearch(chkht, cen->fp, FP_SIZE);

				if (hen != NULL ) {
					bt_insertkey(indb, RAWPTR(bpos), 8, 0, hen->d1,
							(hen->d2 << 16) + 1);
				} else {
					bt_insertkey(indb, RAWPTR(bpos), 8, 0, NODEDUP + p,
							(j << 16) + 1);
					pthread_spin_lock(&service._rlock);
					cen->ref++;
					pthread_spin_unlock(&service._rlock);
				}
			}
		}
		HTDestroy(chkht);
		HTDestroy(seght);
		munmap(dir, size);
		close(fd);

		// Generate Indirect from B-tree
		sprintf(buf, DATA_DIR "image/i%u-%u", i, service._ver);
		fd = open(buf, O_WRONLY | O_CREAT | O_TRUNC, 0644);

		uint64_t index = 0;
		uint32_t slot = bt_startkey(indb, (uint8_t *) &index, sizeof(index));
		Indirect ind, prev;
		prev.ptr = 0;
		prev.offset = 0;
		prev.count = 0;
		do {
			// Should merge with the neighbour entries
			ind.ptr = bt_uid(indb, slot);
			ind.offset = bt_tod(indb, slot) >> 16;
			ind.count = bt_tod(indb, slot) & 0xFFFF;
			if (ind.ptr == prev.ptr && ind.offset == prev.offset + prev.count) {
				prev.count += ind.count;
				continue;
			}
			if (prev.count) {
				assert(write(fd, &prev, sizeof(Indirect)) == sizeof(Indirect));
			}
			prev = ind;
		} while ((slot = bt_nextkey(indb, slot)) != 0);
		assert(write(fd, &prev, sizeof(Indirect)) == sizeof(Indirect));

		close(fd);

		bt_close(indb);
		sprintf(buf, DATA_DIR "image/t%u-%u", i, service._ver);
		unlink(buf);
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
	for (i = 0; i < REV_TCNT; i++) {
		pthread_create(service._tid + i, NULL, process, (void *)i);
	}
	return 0;
}

static int stop() {
	int i, ret;
	for (i = 0; i < REV_TCNT; i++) {
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
