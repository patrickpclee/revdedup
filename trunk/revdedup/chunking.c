/*
 * chunking.c
 *
 *  Created on: Aug 24, 2013
 *      Author: chng
 */

#include <revdedup.h>
#include "rabin.h"
#include "fingerprint.h"

int main(int argc, char * argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Usage : %s filename out\n", argv[0]);
		return 0;
	}
	char * in = argv[1];
	char * out = argv[2];

	int ifd = open(in, O_RDONLY);
	int ofd = creat(out, 0644);
	uint64_t size = lseek(ifd, 0, SEEK_END);
	uint8_t * data = MMAP_FD_RO(ifd, size);
	Queue * q = NewQueue();
	Queue * rfq = NewQueue();

	RabinService * rs = GetRabinService();
	FpService * fs = GetFpService();
	rs->start(data, size, rfq);
	fs->start(rfq, q);

	Segment * seg;
	while ((seg = (Segment *)Dequeue(q)) != NULL) {
		write(ofd, seg, sizeof(Segment));
		free(seg);
	}

	rs->stop();
	fs->stop();
	free(rfq);
	free(q);
	munmap(data, size);

	close(ifd);
	close(ofd);

	return 0;
}
