/**
 * @file 	chunking.c
 * @brief	Program that generates metadata file for further processing
 * @author	Ng Chun Ho
 */

#include <revdedup.h>
#include <openssl/sha.h>
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
#ifdef WITH_REAL_DATA
	uint64_t size = lseek(ifd, 0, SEEK_END);
	uint8_t * data = MMAP_FD_RO(ifd, size);
	Queue * eq = NewQueue();
	Queue * rfq = NewQueue();

	RabinService * rs = GetRabinService();
	FpService * fs = GetFpService();
	rs->start(data, size, rfq);
	fs->start(rfq, eq);

	Segment * seg;
	while ((seg = (Segment *)Dequeue(eq)) != NULL) {
		assert(write(ofd, seg, sizeof(Segment)) == sizeof(Segment));
		free(seg);
	}

	rs->stop();
	fs->stop();
	free(rfq);
	free(eq);
	munmap(data, size);
#else
	uint64_t offset = 0;
	uint64_t size, i;
	uint8_t * fps = malloc(MAX_SEG_CHUNKS * FP_SIZE);
	Segment * seg = malloc(sizeof(Segment));
	while ((size = read(ifd, fps, MAX_SEG_CHUNKS * FP_SIZE)) > 0) {
		seg->offset = offset;
		seg->chunks = size / FP_SIZE;
		seg->len = seg->chunks * AVG_CHUNK_SIZE;

		for (i = 0; i < seg->chunks; i++) {
			memcpy(seg->en[i].fp, fps + i * FP_SIZE, FP_SIZE);
			seg->en[i].pos = i * AVG_CHUNK_SIZE;
			seg->en[i].len = AVG_CHUNK_SIZE;
			seg->en[i].ref = 0;
		}
		SHA1(fps, size, seg->fp);
		offset += AVG_SEG_SIZE;
		assert(write(ofd, seg, sizeof(Segment)) == sizeof(Segment));
	}

	free(seg);
	free(fps);
#endif

	close(ifd);
	close(ofd);

	return 0;
}
