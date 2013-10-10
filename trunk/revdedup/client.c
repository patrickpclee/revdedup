/*
 * client.c
 *
 *  Created on: Oct 9, 2013
 *      Author: chng
 */

/**
 * Client for upload process
 */

#include <revdedup.h>
#include <queue.h>
#include <curl/curl.h>

typedef struct {
	size_t ptr;
	size_t size;
	void * buffer;
} RespBuffer;

static size_t writeFn(void * ptr, size_t size, size_t nmemb, void * userdata) {
	RespBuffer * rb = (RespBuffer *) userdata;
	if (rb) {
		memcpy(rb->buffer + rb->ptr, ptr, size * nmemb);
		rb->ptr += size * nmemb;
	}
	return nmemb;
}

static int post(char * path, void * data, size_t size, void * resp) {
	CURL * curl = curl_easy_init();
	assert(curl != NULL);

	curl_easy_setopt(curl, CURLOPT_POST, 1);
	curl_easy_setopt(curl, CURLOPT_URL, path);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, size);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, resp);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeFn);
	if (size) {
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
	}

	CURLcode ret = curl_easy_perform(curl);
	assert(ret == CURLE_OK);
	curl_easy_cleanup(curl);
	return 0;
}

void * processSegments(void * ptr) {
	Queue * q = (Queue *) ptr;
	Segment * seg;
	uint8_t buf[128];
	while ((seg = (Segment *)Dequeue(q)) != NULL) {
		sprintf(buf, "%s:%u/%lu", HOST, DATA_PORT, seg->id);
		post(buf, seg->data, seg->len, NULL);
	}
	return NULL;
}


int main(int argc, char * argv[]) {
	if (argc != 4) {
		fprintf(stderr, "Usage : %s file metafile instanceID\n", argv[0]);
		return 0;
	}
	uint8_t buf[128];
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

	struct timeval x;
	TIMERSTART(x);
	uint64_t * resp = malloc(entries * sizeof(uint64_t));
	RespBuffer rb = { .ptr = 0, .size = entries * sizeof(uint64_t), .buffer =
			resp };
	sprintf(buf, "%s:%u/%u", HOST, META_PORT, atoi(argv[3]));
	post(buf, base_seg, osize, &rb);

	Queue * segq = LongQueue();
	pthread_t segt[DATA_THREAD_CNT];
	for (i = 0; i < DATA_THREAD_CNT; i++) {
		pthread_create(segt + i, NULL, processSegments, segq);
	}

	for (i = 0; i < entries; i++) {
		if (resp[i] == 0) {
			continue;
		}
		Segment * seg = base_seg + i;
		seg->data = data + seg->offset;
		seg->id = resp[i];
		Enqueue(segq, seg);
	}
	sprintf(buf, "%s:%u/%s", HOST, META_PORT, "sync");
	post(buf, NULL, 0, NULL);

	TIMERSTOP(x);
	printf("%ld.%06ld\n", x.tv_sec, x.tv_usec);

	for (i = 0; i < DATA_THREAD_CNT; i++) {
		Enqueue(segq, NULL);
	}
	for (i = 0; i < DATA_THREAD_CNT; i++) {
		pthread_join(segt[i], NULL);
	}
	DelQueue(segq);

	free(resp);
	munmap(data, isize);
	munmap(base_seg, osize);
	close(ifd);
	close(ofd);
}
