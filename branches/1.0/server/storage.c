/*
 * storage.c
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/sendfile.h>
#include "storage.h"

static StorageService service;

static int createStores(Fingerprint * segmentfp, uint32_t cnt, Fingerprint * blockfps) {
	char name[STRING_BUF_SIZE];
	char * loc = service.locs[hash(segmentfp, 0xFFFF) % service.loc_cnt];
	int fd, ret;
	sprintf(name, "%s/blockfp/%02x/", loc, segmentfp->x[5]);
	bin2hex(segmentfp, name + strlen(name));

	ret = access(name, R_OK | W_OK);
	if (ret) {
		fd = open(name, O_CREAT | O_WRONLY | O_TRUNC, 0644);
		if (fd == -1) {
			perror(__FUNCTION__);
		}
		ssize_t ret = write(fd, blockfps, cnt * sizeof(Fingerprint));
		if (ret == -1) {
			perror(__FUNCTION__);
		}
		close(fd);
	}

	sprintf(name, "%s/segment/%02x/", loc, segmentfp->x[5]);
	bin2hex(segmentfp, name + strlen(name));

	ret = access(name, R_OK | W_OK);
	if (ret) {
		fd = open(name, O_CREAT | O_WRONLY, 0644);
		int ret = ftruncate(fd, SEGSTORE_META_SIZE);
		if (ret == -1) {
			perror(__FUNCTION__);
		}
		close(fd);
	}
	return 0;
}

static SegmentStore * getSegmentStore(Segment * seg) {
	SegmentStore * ss = malloc(sizeof(SegmentStore));
	char name[STRING_BUF_SIZE];
	char * loc = service.locs[hash(&seg->fp, 0xFFFF) % service.loc_cnt];
	int fd;

	if (seg->refcnt != -1) {
		sprintf(name, "%s/segment/%02x/", loc, seg->fp.x[5]);
	} else {
		sprintf(name, "%s/oldsegment/%02x/", loc, seg->fp.x[5]);
	}
	bin2hex(&seg->fp, name + strlen(name));
	fd = open(name, O_RDWR);
	if (fd == -1) {
		free(ss);
		return NULL;
	}

	ss->segment = seg;
	ss->fd = fd;
	ss->si = mmap(NULL, SEGSTORE_META_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	return ss;
}

static void putSegmentStore(SegmentStore * ss) {
	munmap(ss->si, SEGSTORE_META_SIZE);
	close(ss->fd);
	free(ss);
}

static void writeSegment(SegmentStore * ss, BlockFPStore * bs, void * data) {
	ssize_t wn;
	uint32_t lblock = 0;
	uint32_t pblock = SEGSTORE_META_BLOCKS;
	
	lseek(ss->fd, SEGSTORE_META_SIZE, SEEK_SET);
	
	for (lblock = 0; lblock < bs->count; lblock++) {
		if (!FP_IS_ZERO(&bs->blockfps[lblock])) {
			wn = write(ss->fd, data + lblock * BLOCK_SIZE, BLOCK_SIZE);
			if (wn != BLOCK_SIZE) {
				perror(__FUNCTION__);
			}
			ss->si[lblock].ptr = pblock++;
		}
	}
}

#define check_range(x) do { if ((x) < 0 || (x) >= SEG_BLOCKS) perror(__FUNCTION__);} while (0)

static size_t pipeFile(int __out_fd, int __in_fd, off_t *__offset,
		size_t __count) {
	size_t wr = 0, tmp;
	while (wr < __count) {
		tmp = sendfile(__out_fd, __in_fd, __offset, __count - wr);
		if (tmp < 0) {
			perror(__FUNCTION__);
			exit(0);
		}
		wr += tmp;
	}
	return wr;
}

static size_t pipeData(int __fd, __const void *__buf, size_t __n) {
	size_t wr = 0, tmp;
	while (wr < __n) {
		tmp = write(__fd, __buf + wr, __n - wr);
		if (tmp < 0) {
			perror(__FUNCTION__);
			exit(0);
		}
		wr += tmp;
	}
	return wr;
}

static size_t pipeSegment(SegmentStore * ss, int pipefd, uint32_t lblock, uint32_t cnt) {
	char zero_buffer[BLOCK_SIZE];
	size_t sent = 0;
	off_t offset;
	uint32_t s_pblock = 0, c_pblock = SEGSTORE_META_BLOCKS, pcnt = 0;
	uint32_t i;

	memset(zero_buffer, 0, BLOCK_SIZE);
	for (i = lblock; i < lblock + cnt; i++) {
		c_pblock = ss->si[i].ptr;
		// check_range(c_pblock);
		
		// zero block
		if (!c_pblock) {
			if (pcnt) {
				offset = s_pblock * BLOCK_SIZE;
				sent += pipeFile(pipefd, ss->fd, &offset, pcnt * BLOCK_SIZE);
				s_pblock += pcnt;
				pcnt = 0;
			}
			sent += pipeData(pipefd, zero_buffer, BLOCK_SIZE);
			continue;
		} else if (c_pblock == s_pblock + pcnt) {
			pcnt++;
			continue;
		}

		if (pcnt) {
			offset = s_pblock * BLOCK_SIZE;
			sent += pipeFile(pipefd, ss->fd, &offset, pcnt * BLOCK_SIZE);
		}
		s_pblock = c_pblock;
		pcnt = 1;
	}
	if (pcnt) {
		offset = s_pblock * BLOCK_SIZE;
		sent += pipeFile(pipefd, ss->fd, &offset, pcnt * BLOCK_SIZE);
	}
	return sent;
}

static size_t pipeZero(int pipefd) {
	char buffer[BLOCK_SIZE];
	memset(buffer, 0, BLOCK_SIZE);
	return pipeData(pipefd, buffer, BLOCK_SIZE);
}

static BlockFPStore * getBlockFPStore(Segment * seg) {
	BlockFPStore * bs = malloc(sizeof(BlockFPStore));
	char name[STRING_BUF_SIZE];
	char * loc = service.locs[hash(&seg->fp, 0xFFFF) % service.loc_cnt];
	int fd;
	size_t size;

	sprintf(name, "%s/blockfp/%02x/", loc, seg->fp.x[5]);
	bin2hex(&seg->fp, name + strlen(name));
	fd = open(name, O_RDWR);
	if (fd == -1) {
		free(bs);
		return NULL;
	}

	bs->segment = seg;
	bs->fd = fd;
	size = lseek(fd, 0, SEEK_END);
	bs->blockfps = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	bs->count = size / sizeof(Fingerprint);
	return bs;
}

static void putBlockFPStore(BlockFPStore * bs) {
	munmap(bs->blockfps, bs->count * sizeof(Fingerprint));
	close(bs->fd);
	free(bs);
}

static void purgeBlockFPStore(Segment * seg) {
	char name[STRING_BUF_SIZE];
	char * loc = service.locs[hash(&seg->fp, 0xFFFF) % service.loc_cnt];
	sprintf(name, "%s/blockfp/%02x/", loc, seg->fp.x[5]);
	bin2hex(&seg->fp, name + strlen(name));
	unlink(name);
}

static VersionMap * createVersionMap(ImageVersion * iv) {
	VersionMap * map = malloc(sizeof(VersionMap));
	char name[STRING_BUF_SIZE];

	sprintf(name, "%s/vmap/%s-%lu", service.locs[iv->version % service.loc_cnt],
			iv->img->name, iv->version);
	map->fd = open(name, O_RDWR | O_CREAT | O_TRUNC, 0644);
	if (map->fd == -1) {
		free(map);
		return NULL ;
	}
	map->iv = iv;
	map->blocks = iv->size / BLOCK_SIZE;
	int ret = ftruncate(map->fd, map->blocks * sizeof(int32_t));
	if (ret == -1) {
		perror(__FUNCTION__);
	}

	map->ptr = mmap(NULL, map->blocks * sizeof(int32_t), PROT_READ | PROT_WRITE,
			MAP_SHARED, map->fd, 0);
	printf("Create VersionMap fd = %d ptr = %p\n", map->fd, map->ptr);
	return map;
}
static VersionMap * getVersionMap(ImageVersion * iv) {
	VersionMap * map = malloc(sizeof(VersionMap));
	char name[STRING_BUF_SIZE];

	sprintf(name, "%s/vmap/%s-%lu", service.locs[iv->version % service.loc_cnt],
			iv->img->name, iv->version);
	map->fd = open(name, O_RDWR, 0644);
	if (map->fd == -1) {
		free(map);
		return NULL ;
	}
	map->iv = iv;
	map->blocks = iv->size / BLOCK_SIZE;
	posix_fadvise(map->fd, 0, map->blocks * sizeof(int32_t),
			POSIX_FADV_WILLNEED);

	map->ptr = mmap(NULL, map->blocks * sizeof(int32_t), PROT_READ | PROT_WRITE,
			MAP_SHARED, map->fd, 0);

	return map;
}
static void putVersionMap(VersionMap * map) {
	munmap(map->ptr, map->blocks * sizeof(int32_t));
	close(map->fd);
	free(map);
}

static void * prefetchSegmentFn(void * cls) {
	while (1) {
		Segment * seg = queue_pop(service.segment_prefetch_q);
		if (!seg) {
			break;
		}
		char name[STRING_BUF_SIZE];
		char * loc = service.locs[hash(&seg->fp, 0xFFFF) % service.loc_cnt];
		int fd;
		off_t len;

		if (seg->refcnt != -1) {
			sprintf(name, "%s/segment/%02x/", loc, seg->fp.x[5]);
		} else {
			sprintf(name, "%s/oldsegment/%02x/", loc, seg->fp.x[5]);
		}
		bin2hex(&seg->fp, name + strlen(name));
		fd = open(name, O_RDWR);
		if (fd == -1) {
			continue;
		}
		len = lseek(fd, 0, SEEK_END);
		posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
		close(fd);
	}

	return NULL;
}

static void prefetchSegmentStore(Segment * seg) {
	static Segment * prev_seg = NULL;
	if (prev_seg == seg) {
		return;
	}
	prev_seg = seg;
	queue_push(seg, service.segment_prefetch_q);
}

static void * prefetchBlockFPFn(void * cls) {
	while (1) {
		Segment * seg = queue_pop(service.blockfp_prefetch_q);
		if (!seg) {
			break;
		}
		char name[STRING_BUF_SIZE];
		char * loc = service.locs[hash(&seg->fp, 0xFFFF) % service.loc_cnt];
		int fd;
		off_t len;

		sprintf(name, "%s/blockfp/%02x/", loc, seg->fp.x[5]);
		bin2hex(&seg->fp, name + strlen(name));
		fd = open(name, O_RDWR);
		if (fd == -1) {
			continue;
		}
		len = lseek(fd, 0, SEEK_END);
		posix_fadvise(fd, 0, len, POSIX_FADV_WILLNEED);
		close(fd);
	}

	return NULL;
}

static void prefetchBlockFPStore(Segment * seg) {
	static Segment * prev_seg = NULL;
	if (prev_seg == seg) {
		return;
	}
	prev_seg = seg;
	queue_push(seg, service.blockfp_prefetch_q);
}


static void * punchSegmentStoreFn(void * cls) {
	StorageService * sts = getStorageService();
	uint32_t i;
	uint32_t punch_cnt;
	char buffer[BLOCK_SIZE];
	while (1) {
		SegmentStore * ss = queue_pop(service.segment_punch_q);
		if (!ss) {
			break;
		}
		printf("Punching ");
		printhex(&ss->segment->fp);

		ss->segment->refcnt = -1;
		punch_cnt = 0;
		for (i = 0; i < SEG_BLOCKS; i++) {
			if (ss->si[i].ptr && !ss->si[i].ref) {
				punch_cnt++;
			}
		}
		if (punch_cnt > SEGSTORE_PUNCH_LIMIT) {
			char name1[STRING_BUF_SIZE], name2[STRING_BUF_SIZE];
			char * loc = service.locs[hash(&ss->segment->fp, 0xFFFF)
					% service.loc_cnt];

			sprintf(name1, "%s/oldsegment/%02x/", loc, ss->segment->fp.x[5]);
			bin2hex(&ss->segment->fp, name1 + strlen(name1));
			sprintf(name2, "%s/segment/%02x/", loc, ss->segment->fp.x[5]);
			bin2hex(&ss->segment->fp, name2 + strlen(name2));

			int fd = open(name1, O_RDWR | O_CREAT | O_TRUNC, 0644);
			int ret = ftruncate(fd, SEGSTORE_META_SIZE);
			if (ret == -1) {
				perror(__FUNCTION__);
			}
			SegmentStoreIndex * si = mmap(NULL, SEGSTORE_META_SIZE,
					PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
			uint32_t pblock = SEGSTORE_META_BLOCKS;
			lseek(fd, SEGSTORE_META_SIZE, SEEK_SET);

			for (i = 0; i < SEG_BLOCKS; i++) {
				if (!ss->si[i].ptr || !ss->si[i].ref) {
					continue;
				}
				ssize_t rn = pread(ss->fd, buffer, BLOCK_SIZE, ss->si[i].ptr * BLOCK_SIZE);
				if (rn == -1) {
					perror(__FUNCTION__);
				}
				ssize_t wn = write(fd, buffer, BLOCK_SIZE);
				if (wn == -1) {
					perror(__FUNCTION__);
				}
				si[i].ptr = pblock++;
				si[i].ref = ss->si[i].ref;
			}

			munmap(si, SEGSTORE_META_SIZE);
			close(fd);

			sts->putSegmentStore(ss);

			unlink(name2);

		} else {	// Punch instead of rebuild
			char name1[STRING_BUF_SIZE], name2[STRING_BUF_SIZE];
			char * loc = service.locs[hash(&ss->segment->fp, 0xFFFF)
								% service.loc_cnt];
			sprintf(name1, "%s/oldsegment/%02x/", loc, ss->segment->fp.x[5]);
			bin2hex(&ss->segment->fp, name1 + strlen(name1));
			sprintf(name2, "%s/segment/%02x/", loc, ss->segment->fp.x[5]);
			bin2hex(&ss->segment->fp, name2 + strlen(name2));
			
			uint32_t sblk = 0, cnt = 0;
			
			for (i = 0; i < SEG_BLOCKS; i++) {
				if (ss->si[i].ptr && !ss->si[i].ref) {
					if (ss->si[i].ptr == sblk + cnt) {
						cnt++;
					} else {
						if (cnt) {
							fallocate(ss->fd, 3, sblk * BLOCK_SIZE, cnt * BLOCK_SIZE);
						}
						sblk = ss->si[i].ptr;
						cnt = 1;
					}
					ss->si[i].ptr = 0;
				}
			}
			if (cnt) {
				fallocate(ss->fd, 3, sblk * BLOCK_SIZE, cnt * BLOCK_SIZE);				
			}
			sts->putSegmentStore(ss);
			rename(name2, name1);
		}
	}

	return NULL;
}

static void punchSegmentStore(SegmentStore * ss) {
	queue_push(ss, service.segment_punch_q);
}

#ifdef TDSERVER_PRESERVE

static pthread_spinlock_t segstore_ps_lock;
static LIST_HEAD(segstore_ps_list);
static uint32_t segstore_ps_cnt = 0;

static void preserveSegmentStore(SegmentStore * ss) {
	pthread_spin_lock(&segstore_ps_lock);
	if (segstore_ps_cnt >= SEGSTORE_PRESERVE_LIMIT) {
		SegmentStore * ptr = list_entry(segstore_ps_list.prev, SegmentStore, ps_head);
		list_del(&ptr->ps_head);
		list_add(&ss->ps_head, &segstore_ps_list);
		pthread_spin_unlock(&segstore_ps_lock);
		putSegmentStore(ptr);
		return;
	}
	list_add(&ss->ps_head, &segstore_ps_list);
	segstore_ps_cnt++;
	pthread_spin_unlock(&segstore_ps_lock);
	return;
}

static SegmentStore * getPreservedSegmentStore(Segment * seg) {
	SegmentStore * ptr;
	pthread_spin_lock(&segstore_ps_lock);
	list_for_each_entry(ptr, &segstore_ps_list, ps_head) {
		if (ptr->segment == seg) {
			list_del(&ptr->ps_head);
			segstore_ps_cnt--;
			pthread_spin_unlock(&segstore_ps_lock);
			return ptr;
		}
	}
	pthread_spin_unlock(&segstore_ps_lock);
	return getSegmentStore(seg);
}

#endif

static void start(uint32_t loc_cnt, char * locs[]) {
	service.loc_cnt = loc_cnt;
	service.locs = locs;
	char tmpstr[STRING_BUF_SIZE];
	uint32_t i;
	for (i = 0; i < loc_cnt; i++) {
		sprintf(tmpstr, "%s/segment/", locs[i]);
		mkdir(tmpstr, 0755);
		sprintf(tmpstr, "%s/blockfp/", locs[i]);
		mkdir(tmpstr, 0755);
		sprintf(tmpstr, "%s/vmap/", locs[i]);
		mkdir(tmpstr, 0755);
	}


	service.segment_prefetch_q = queue_create();
	service.blockfp_prefetch_q = queue_create();
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		pthread_create(&service.segment_prefetch_t[i], NULL, prefetchSegmentFn,
			NULL );
	}
	for (i = 0; i < BLKSTORE_PREFETCH_THREAD; i++) {
		pthread_create(&service.blockfp_prefetch_t[i], NULL, prefetchBlockFPFn,
			NULL );
	}

	service.segment_punch_q = queue_create();
	for (i = 0; i < SEGSTORE_PUNCH_THREAD; i++) {
		pthread_create(&service.segment_punch_t[i], NULL, punchSegmentStoreFn,
				NULL );
	}
	// pthread_spin_init(&segstore_ps_lock, PTHREAD_PROCESS_PRIVATE);
}

static void stop() {
	uint32_t i;
	for (i = 0; i < SEGSTORE_PUNCH_THREAD; i++) {
		queue_push(NULL, service.segment_punch_q);
	}
	for (i = 0; i < SEGSTORE_PUNCH_THREAD; i++) {
		pthread_join(service.segment_punch_t[i], NULL);
	}
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		queue_push(NULL, service.blockfp_prefetch_q);
	}
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		pthread_join(service.blockfp_prefetch_t[i], NULL);
	}
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		pthread_join(service.segment_prefetch_t[i], NULL);
	}
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		queue_push(NULL, service.segment_prefetch_q);
	}
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		pthread_join(service.segment_prefetch_t[i], NULL);
	}
	queue_destroy(service.blockfp_prefetch_q);
	queue_destroy(service.segment_prefetch_q);
}

static StorageService service = {
		.start = start,
		.stop = stop,
		.createStores = createStores,
		.getSegmentStore = getSegmentStore,
		.putSegmentStore = putSegmentStore,
		.writeSegment = writeSegment,
		.pipeSegment = pipeSegment,
		.pipeZero = pipeZero,
		.getBlockFPStore = getBlockFPStore,
		.putBlockFPStore = putBlockFPStore,
		.purgeBlockFPStore = purgeBlockFPStore,
		.createVersionMap = createVersionMap,
		.getVersionMap = getVersionMap,
		.putVersionMap = putVersionMap,
		.prefetchSegmentStore = prefetchSegmentStore,
		.prefetchBlockFPStore = prefetchBlockFPStore,
		.punchSegmentStore = punchSegmentStore,
};

StorageService * getStorageService() {
	return &service;
}
