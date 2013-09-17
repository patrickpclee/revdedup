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

static SegmentStore * getSegmentStore(Segment * seg) {
	SegmentStore * ss = malloc(sizeof(SegmentStore));
	char name[STRING_BUF_SIZE];
	char * loc = service.locs[hash(&seg->fp, 0xFFFF) % service.loc_cnt];
	int fd;

	sprintf(name, "%s/segment/%02x/", loc, seg->fp.x[5]);

	bin2hex(&seg->fp, name + strlen(name));
	fd = open(name, O_RDWR | O_CREAT, 0644);
	if (fd == -1) {
		free(ss);
		return NULL;
	}

	ss->segment = seg;
	ss->fd = fd;
	return ss;
}

static void putSegmentStore(SegmentStore * ss) {
	close(ss->fd);
	free(ss);
}

static void writeSegment(SegmentStore * ss, void * data) {
	ssize_t wn;

	wn = write(ss->fd, data, SEG_SIZE);
	if (wn != SEG_SIZE) {
		perror(__FUNCTION__);
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

static size_t pipeSegment(SegmentStore * ss, int pipefd, uint32_t lblock, uint32_t cnt) {
	size_t sent = 0;
	off_t offset = 0;

	sent += pipeFile(pipefd, ss->fd, &offset, SEG_SIZE);
	return sent;
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

		sprintf(name, "%s/segment/%02x/", loc, seg->fp.x[5]);
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
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		pthread_create(&service.segment_prefetch_t[i], NULL, prefetchSegmentFn,
			NULL );
	}
}

static void stop() {
	uint32_t i;
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		queue_push(NULL, service.segment_prefetch_q);
	}
	for (i = 0; i < SEGSTORE_PREFETCH_THREAD; i++) {
		pthread_join(service.segment_prefetch_t[i], NULL);
	}
	queue_destroy(service.segment_prefetch_q);
}

static StorageService service = {
		.start = start,
		.stop = stop,
		.getSegmentStore = getSegmentStore,
		.putSegmentStore = putSegmentStore,
		.writeSegment = writeSegment,
		.pipeSegment = pipeSegment,
		.prefetchSegmentStore = prefetchSegmentStore,
};

StorageService * getStorageService() {
	return &service;
}
