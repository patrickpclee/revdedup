/*
 * main.c
 *
 *  Created on: May 28, 2013
 *      Author: chng
 */

#include <revdedup.h>
#include <sys/time.h>
#include "index.h"
#include "image.h"
#include "compress.h"
#include "bucket.h"

Queue * mmq;

void * end(void * ptr) {
	Queue * q = (Queue *)ptr;
	Segment * seg;
	while ((seg = (Segment *)Dequeue(q)) != NULL) {
		Enqueue(mmq, seg->cdata);
	}
	sync();
	return NULL;
}

int main(int argc, char * argv[]) {
	if (argc != 4) {
		fprintf(stderr, "Usage : %s filename out instanceID\n", argv[0]);
		return 0;
	}
	uint64_t i;

	int ifd = open(argv[1], O_RDONLY);
	int ofd = open(argv[2], O_RDONLY);
	assert(ifd != -1);
	assert(ofd != -1);

	posix_fadvise(ofd, 0, 0, POSIX_FADV_WILLNEED);
	uint64_t isize = lseek(ifd, 0, SEEK_END);
	uint64_t osize = lseek(ofd, 0, SEEK_END);
	uint64_t entries = osize / sizeof(Segment);
	uint8_t * data = MMAP_FD_RO(ifd, isize);
	Segment * base_seg = MMAP_FD_PV(ofd, osize);

	mmq = LongQueue();
	void * cdata = MMAP_MM(MAX_COMPRESSED_SIZE * LONGQUEUE_LENGTH);
	for (i = 0; i < LONGQUEUE_LENGTH; i++) {
		Enqueue(mmq, cdata + i * MAX_COMPRESSED_SIZE);
	}

	IndexService * is = GetIndexService();
	ImageService * es = GetImageService();
	CompressService * cs = GetCompressService();
	BucketService * bs = GetBucketService();


	Queue * miq = NewQueue();
	Queue * icq = NewQueue();
	Queue * ceq = NewQueue();
	Queue * ebq = NewQueue();
	Queue * bmq = NewQueue();

	is->start(miq, icq);
	cs->start(icq, ceq);
	es->start(ceq, ebq, atoi(argv[3]));
	bs->start(ebq, bmq);
	pthread_t mid;
	pthread_create(&mid, NULL, end, bmq);

	struct timeval x;
	TIMERSTART(x);

	for (i = 0; i < entries; i++) {
		Segment * seg = base_seg + i;
		seg->data = data + seg->offset;
		seg->cdata = Dequeue(mmq);
		Enqueue(miq, seg);
	}
	Enqueue(miq, NULL);

	pthread_join(mid, NULL);
	TIMERSTOP(x);
	printf("%ld.%06ld\n", x.tv_sec, x.tv_usec);

	is->stop();
	cs->stop();
	es->stop();
	bs->stop();

	free(miq);
	free(icq);
	free(ceq);
	free(ebq);
	free(bmq);

	munmap(data, isize);
	munmap(base_seg, osize);
	munmap(cdata, MAX_COMPRESSED_SIZE * LONGQUEUE_LENGTH);
	free(mmq);
	close(ifd);
	close(ofd);

	return 0;
}
