
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <jansson.h>
#include <blockqueue.h>
#include "rdclient.h"
#include "httprequest.h"
#include "fpcompute.h"

#define EXPAND_FP_SIZE 20
#define FP_ZERO(x) (!memcmp((x), "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d", EXPAND_FP_SIZE))


int main(int argc, const char * argv[]) {
	if (argc != 3) {
		printf("Usage : %s filename instance \n", argv[0]);
		return 0;
	}

	uint64_t i;
	Image img;
	img.name = argv[2];
	img.fd = open(argv[1], O_RDONLY);

	img.size = lseek(img.fd, 0, SEEK_END);
	img.blocks = img.size / BLOCK_SIZE;
	img.segcnt = (img.size - 1) / SEG_SIZE + 1;
	img.segments = malloc(img.segcnt * sizeof(Segment));
	lseek(img.fd, 0, SEEK_SET);
	void * data = mmap(NULL, img.size,
	PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, img.fd, 0);
	/*
	 * Compute Fingerprints with expand
	 */
	fprintf(stderr, "Computing Fingerprints\n");
	Queue * fpcomp_q = queue_create();
	FPCompService * fcs = getFPCompService();
	fcs->start(fpcomp_q, NULL);

	for (i = 0; i < img.segcnt; i++) {
		img.segments[i].id = i;
		img.segments[i].data = data + i * SEG_SIZE;
		img.segments[i].blocks = SEG_BLOCKS;
		if (i == img.segcnt - 1)
			img.segments[i].blocks = (img.blocks - 1) % SEG_BLOCKS + 1;
		queue_push(&img.segments[i], fpcomp_q);
	}

	fcs->stop(fpcomp_q);
	close(img.fd);
	
	/*
	 * Compute Fingerprint End
	 */
	
	fprintf(stderr, "Start uploading\n");

	struct timeval a, b, c;
	gettimeofday(&a, NULL);
	SegmentService * ss = getSegmentService();
	ss->segmentExist(img.name, img.segcnt, img.segments);

	Queue * putseg_q = queue_create();

	ss->start(putseg_q, NULL );
	for (i = 0; i < img.segcnt; i++) {
		if (!img.segments[i].exist) {
			queue_push(&img.segments[i], putseg_q);
		}
	}
	ss->stop();

	ss->putImage(&img);
	// sync();
	gettimeofday(&b, NULL);
	timersub(&b, &a, &c);
	printf("Time = %ld.%06ld\n", c.tv_sec, c.tv_usec);
	munmap(data, img.size);
	free(img.segments);
	return 0;
}
