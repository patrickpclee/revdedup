/*
 * file.c
 *
 *  Created on: May 30, 2013
 *      Author: chng
 */


#include "image.h"
#include "fingerprint.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


static ImageService service;

static void * process(void * ptr) {
	while (1) {
		Chunk * ch = (Chunk *)Dequeue(service._iq);
		if (ch == NULL) {
			break;
		}
		service._size += ch->len;
		service._csize += ch->clen;
		service._space += ch->unique ? ch->len : 0;
		service._cspace += ch->unique ? ch->clen : 0;

		write(service._fd, ch, sizeof(Direct));

		Enqueue(service._oq, ch);
	}
	Enqueue(service._oq, NULL);
	return NULL;
}

static int start(Queue * iq, Queue * oq) {
	int ret, fd;
	char buf[64];
	service._iq = iq;
	service._oq = oq;
	service._size = 0;
	service._csize = 0;
	service._space = 0;
	service._cspace = 0;

	/* Load Image Log */
	fd = open(DATA_DIR "ilog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, MAX_ENTRIES * sizeof(IMEntry)));
	service._en = MMAP_FD(fd, MAX_ENTRIES * sizeof(IMEntry));
	service._log = (ImageLog *) service._en;
	close(fd);

	service._id = ++service._log->imageID;

	sprintf(buf, DATA_DIR "image/%lu", service._id);
	service._fd = open(buf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
	ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

static int stop() {
	int ret = pthread_join(service._tid, NULL);
	close(service._fd);
	service._en[service._id].size = service._size;
	service._en[service._id].space = service._space;
	service._en[service._id].csize = service._csize;
	service._en[service._id].cspace = service._cspace;
	munmap(service._en, MAX_ENTRIES * sizeof(IMEntry));

	return ret;
}

static ImageService service = {
		.start = start,
		.stop = stop,
};

ImageService* GetImageService() {
	return &service;
}
