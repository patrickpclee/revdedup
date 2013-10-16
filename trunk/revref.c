/**
 * @file	revref.c
 * @brief	Reverse Deduplication Reference Service Implementation
 * @author	Ng Chun Ho
 */

#include "revref.h"

static RevRefService service;

/**
 * Main loop for processing segment references
 * @param ptr
 */
static void * process(void * ptr) {
	uint32_t i;
	char buf[64];
	Direct en;
	for (i = 0; i < service._ins; i++) {
		sprintf(buf, DATA_DIR "image/%u-%u", i, service._ver);
		int fd = open(buf, O_RDONLY);
		while (read(fd, &en, sizeof(Direct)) > 0) {
			service._sen[en.id].ref--;
		}
		close(fd);
	}
	return NULL;
}

/**
 * Implements RevRefService->start()
 */
static int start(SMEntry * sen, uint32_t images, uint32_t version) {
	service._sen = sen;
	service._slog = (SegmentLog *)sen;
	service._ins = images;
	service._ver = version;
	int ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

/**
 * Implements RevRefService->stop()
 */
static int stop() {
	pthread_join(service._tid, NULL);
	return 0;
}

static RevRefService service = {
		.start = start,
		.stop = stop
};

RevRefService * GetRevRefService() {
	return &service;
}
