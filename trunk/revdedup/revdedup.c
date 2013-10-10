/*
 * revdedup.c
 *
 *  Created on: Jun 14, 2013
 *      Author: chng
 */

#include <sys/time.h>
#include <revdedup.h>
#include "revref.h"
#include "revmap.h"
#include "revrbd.h"

int main(int argc, char * argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Usage : %s insts version\n", argv[0]);
		return 0;
	}

	int fd, i;

	fd = open(DATA_DIR "ilog", O_RDWR);
	IMEntry * ien = MMAP_FD(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	fd = open(DATA_DIR "slog", O_RDWR);
	SMEntry * sen = MMAP_FD(fd, MAX_ENTRIES(sizeof(SMEntry)));
	close(fd);

	fd = open(DATA_DIR "clog", O_RDWR);
	CMEntry * cen = MMAP_FD(fd, MAX_ENTRIES(sizeof(CMEntry)));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDWR);
	BMEntry * ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	close(fd);

	uint32_t ins = atoi(argv[1]);
	int32_t ver = atoi(argv[2]);

	if (ver >= 0) {
		for (i = 0; i < ins; i++) {
			ien[i].recent--;
			ien[i].old++;
		}

		struct timeval x;
		TIMERSTART(x);
		RevRefService * rrs = GetRevRefService();
		rrs->start(sen, ins, ver);
		rrs->stop();

		RevMapService * rms = GetRevMapService();
		rms->start(sen, cen, ins, ver);
		rms->stop();

		RevRbdService * rbs = GetRevRbdService();
		rbs->start(sen, cen, ben, ver);
		rbs->stop();
		sync();
		TIMERSTOP(x);
		printf("%ld.%06ld\n", x.tv_sec, x.tv_usec);
	} else {
		printf("%ld.%06ld\n", 0L, 0L);
	}

	munmap(ien, INST_MAX(sizeof(IMEntry)));
	munmap(sen, MAX_ENTRIES(sizeof(SMEntry)));
	munmap(cen, MAX_ENTRIES(sizeof(CMEntry)));
	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));
	return 0;
}
