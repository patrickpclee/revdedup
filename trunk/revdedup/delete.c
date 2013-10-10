/*
 * delete.c
 *
 *  Created on: 14 Aug, 2013
 *      Author: ngchunho
 */

#include <revdedup.h>
#include <sys/time.h>

int main(int argc, char * argv[]) {
	if (argc != 2) {
		fprintf(stderr, "Usage: %s version\n", argv[0]);
		return 0;
	}
	char buf[128];
	uint32_t insts, version = atoi(argv[1]);;
	int fd;
	uint32_t i;
	uint64_t j;
	uint64_t fsize = 0;

	fd = open(DATA_DIR "blog", O_RDWR);
	BMEntry * ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	BucketLog * blog = (BucketLog *) ben;
	close(fd);

	fd = open(DATA_DIR "ilog", O_RDWR);
	ssize_t isize = lseek(fd, 0, SEEK_END);
	IMEntry * ien = MMAP_FD(fd, isize);
	insts = isize / sizeof(IMEntry);
	close(fd);

	for (i = 0; i < insts; i++) {
		sprintf(buf, DATA_DIR "image/%u-%u", i, version);
		unlink(buf);
		sprintf(buf, DATA_DIR "image/i%u-%u", i, version);
		unlink(buf);
		ien[i].old--;
		ien[i].deleted++;

	}

	for (j = 1; j <= blog->bucketID; j++) {
		if (ben[j].ver == version) {
			sprintf(buf, DATA_DIR "bucket/%08lx", j);
			unlink(buf);
			fsize += ben[j].size - ben[j].psize;
			ben[j].size = 0;
			ben[j].psize = 0;
		}
	}
	sync();

	munmap(ien, isize);
	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));
	return 0;
}
