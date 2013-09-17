/*
 * image.c
 *
 *  Created on: May 30, 2013
 *      Author: chng
 */

#include "image.h"
#include "fingerprint.h"

static ImageService service;

static void * process(void * ptr) {
	uint32_t ins = service._ins;
	uint32_t ver = service._ver;
	Segment * seg;
	while ((seg = (Segment *) Dequeue(service._iq)) != NULL) {
		service._en[ins].vers[ver].size += seg->len;
		service._en[ins].vers[ver].space += seg->unique ? seg->len : 0;
		service._en[ins].vers[ver].csize += seg->clen;
		service._en[ins].vers[ver].cspace += seg->unique ? seg->clen : 0;

		service._cspace += seg->unique ? seg->clen : 0;

		write(service._fd, seg, 16);

		Enqueue(service._oq, seg);
	}
	Enqueue(service._oq, NULL);

	return NULL;
}

static int start(Queue * iq, Queue * oq, uint32_t instanceID) {
	int ret, fd;
	char buf[128];
	service._iq = iq;
	service._oq = oq;

	fd = open(DATA_DIR "ilog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, INST_MAX * sizeof(IMEntry)));
	service._en = MMAP_FD(fd, INST_MAX * sizeof(IMEntry));
	close(fd);

	service._ins = instanceID;
	service._ver = service._en[instanceID].versions++;
	service._en[instanceID].recent++;

	service._cspace = 0;

	sprintf(buf, DATA_DIR "image/%u-%u", service._ins, service._ver);
	service._fd = creat(buf, 0644);
	ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

static int stop() {
	int ret = pthread_join(service._tid, NULL);
	close(service._fd);

	munmap(service._en, INST_MAX * sizeof(SMEntry));
	return ret;
}

static ImageService service = {
		.start = start,
		.stop = stop,
};

ImageService* GetImageService() {
	return &service;
}
