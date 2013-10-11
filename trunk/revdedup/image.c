/**
 * @file	image.c
 * @brief	Image Service Implementation
 * @author	Ng Chun Ho
 */

#include "image.h"

static ImageService service;

/**
 * Main loop for processing segments
 * @param ptr		useless
 */
static void * process(void * ptr) {
	uint32_t ins = service._ins;
	uint32_t ver = service._ver;
	Segment * seg;
	while ((seg = (Segment *) Dequeue(service._iq)) != NULL) {
		service._en[ins].vers[ver].size += seg->len;
		service._en[ins].vers[ver].space += seg->unique ? seg->len : 0;
		service._en[ins].vers[ver].csize += seg->clen;
		service._en[ins].vers[ver].cspace += seg->unique ? seg->clen : 0;

		assert(write(service._fd, seg, sizeof(Direct)) == sizeof(Direct));

		Enqueue(service._oq, seg);
	}
	Enqueue(service._oq, NULL);

	return NULL;
}

/**
 * Implements ImageService->start()
 */
static int start(Queue * iq, Queue * oq, uint32_t imageID) {
	int ret, fd;
	char buf[128];
	service._iq = iq;
	service._oq = oq;

	fd = open(DATA_DIR "ilog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, INST_MAX(sizeof(IMEntry))));
	service._en = MMAP_FD(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	service._ins = imageID;
	service._ver = service._en[imageID].versions++;
	service._en[imageID].recent++;

	sprintf(buf, DATA_DIR "image/%u-%u", service._ins, service._ver);
	service._fd = creat(buf, 0644);
	ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

/**
 * Implements ImageService->stop()
 */
static int stop() {
	int ret = pthread_join(service._tid, NULL);
	close(service._fd);
	munmap(service._en, INST_MAX(sizeof(IMEntry)));
	return ret;
}

static ImageService service = {
		.start = start,
		.stop = stop,
};

ImageService* GetImageService() {
	return &service;
}
