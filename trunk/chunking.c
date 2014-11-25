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
		fprintf(stderr, "Usage : %s <input file> <output chunking metafile>\n", argv[0]);
		return 0;
	}
	char * in = argv[1];
	char * out = argv[2];

	int ifd = open(in, O_RDONLY);
	int ofd = creat(out, 0644);
#ifdef WITH_REAL_DATA
	uint64_t size = lseek(ifd, 0, SEEK_END);
	posix_fadvise(ifd,0,size,POSIX_FADV_WILLNEED);
	uint8_t * data = MMAP_FD_RO(ifd, size);

#if (CHUNK_SHIFT == 0) // Fixed Size Chunking
//	printf("Fixed Size Chunking\n");
	uint64_t count = size / AVG_SEG_SIZE;
	Segment * seg = malloc(sizeof(Segment));

	uint64_t offset = 0;
	int j = 0;
	uint64_t i = 0;
	for (i = 0; i < count; ++i) {
		memset(seg, 0, sizeof(Segment));
		seg->data = data + offset;
		seg->offset = offset;
		seg->chunks = AVG_SEG_BLOCKS;
		seg->len = AVG_SEG_SIZE;

		for (j = 0; j < AVG_SEG_BLOCKS; ++j) {
			uint8_t* chunk_data = seg->data + AVG_CHUNK_SIZE * j;
			seg->en[j].pos = j * AVG_CHUNK_SIZE;
			seg->en[j].len = AVG_CHUNK_SIZE;
			seg->en[j].ref = 0;
			SHA1(chunk_data, AVG_CHUNK_SIZE, seg->en[j].fp);
		}

		SHA1(seg->data, seg->len, seg->fp);

		offset += AVG_SEG_SIZE;
		assert(write(ofd, seg, sizeof(Segment)) == sizeof(Segment));
	}

	// Last Segment
	if (offset < size) {
		memset(seg, 0, sizeof(Segment));
		seg->data = data + offset;
		seg->offset = offset;
		seg->len = size - offset;
		seg->chunks = seg->len / AVG_CHUNK_SIZE;
		if (seg->len % AVG_CHUNK_SIZE != 0)
			seg->chunks++;

		for (j = 0; j < seg->chunks; ++j) {
			uint8_t * chunk_data = seg->data + AVG_CHUNK_SIZE * j;
			seg->en[j].pos = j * AVG_CHUNK_SIZE;
			seg->en[j].ref = 0;
			int chunkSize = AVG_CHUNK_SIZE;
			if(j == (seg->chunks - 1)) 
				chunkSize = (size - offset) % AVG_CHUNK_SIZE;
			seg->en[j].len = chunkSize;

			SHA1(chunk_data,chunkSize, seg->en[j].fp);
		}
		assert(write(ofd, seg, sizeof(Segment)) == sizeof(Segment));
	}

	free(seg);

	
#else // Variable Size Chunking
	Queue * eq = NewQueue();
	Queue * rfq = NewQueue();

	RabinService * rs = GetRabinService();
	FpService * fs = GetFpService();
	rs->start(data, size, rfq);
	fs->start(rfq, eq);

	Segment * seg;
	while ((seg = (Segment *)Dequeue(eq)) != NULL) {
		SHA1(seg->data, seg->len, seg->fp);
		assert(write(ofd, seg, sizeof(Segment)) == sizeof(Segment));
		free(seg);
	}

	rs->stop();
	fs->stop();

	free(rfq);
	free(eq);
#endif
	munmap(data, size);
#else
	uint64_t offset = 0;
	uint64_t size, i;
	uint8_t * fps = malloc(MAX_SEG_CHUNKS * FP_SIZE);
	Segment * seg = malloc(sizeof(Segment));
	//uint64_t ccid = 0;
	while ((size = read(ifd, fps, MAX_SEG_CHUNKS * FP_SIZE)) > 0) {
		seg->offset = offset;
		seg->chunks = size / FP_SIZE;
		seg->len = seg->chunks * AVG_CHUNK_SIZE;
		//seg->cid = ccid;

		for (i = 0; i < seg->chunks; i++) {
			memcpy(seg->en[i].fp, fps + i * FP_SIZE, FP_SIZE);
			seg->en[i].pos = i * AVG_CHUNK_SIZE;
			seg->en[i].len = AVG_CHUNK_SIZE;
			seg->en[i].ref = 0;
		}
		SHA1(fps, size, seg->fp);
		offset += AVG_SEG_SIZE;
		assert(write(ofd, seg, sizeof(Segment)) == sizeof(Segment));
		//ccid += seg->chunks;
	}

	free(seg);
	free(fps);
#endif

	close(ifd);
	close(ofd);

	return 0;
}
