/*
 * revref.c
 *
 *  Created on: Jun 14, 2013
 *      Author: chng
 */

#include "revref.h"

static RevRefService service;

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

static int start(SMEntry * sen, uint32_t instances, uint32_t version) {
	service._sen = sen;
	service._slog = (SegmentLog *)sen;
	service._ins = instances;
	service._ver = version;
	int ret = pthread_create(&service._tid, NULL, process, NULL);
	return ret;
}

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
