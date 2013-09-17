/*
 * remove.c
 *
 *  Created on: Aug 21, 2013
 *      Author: chng
 */

#include <convdedup.h>

CMEntry * cen;
BMEntry * ben;

int main(int argc, char * argv[]) {
	if (argc != 2) {
		fprintf(stderr, "Usage : %s ID\n", argv[0]);
		return 0;
	}
	char buf[128];
	uint32_t inst = atoi(argv[1]);
	int fd;
	fd = open(DATA_DIR "clog", O_RDWR);
	cen = MMAP_FD(fd, MAX_ENTRIES * sizeof(CMEntry));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDWR);
	ben = MMAP_FD(fd, MAX_ENTRIES * sizeof(BMEntry));
	close(fd);

	sprintf(buf, DATA_DIR "image/%u", inst);
	fd = open(buf, O_RDONLY);

	Direct dir;
	while (read(fd, &dir, sizeof(dir)) > 0) {
		cen[dir.id].ref--;
		if (cen[dir.id].ref == 0) {
			ben[cen[dir.id].bucket].rsize += cen[dir.id].len;
		}
	}
	close(fd);
	unlink(buf);

	munmap(ben, MAX_ENTRIES * sizeof(BMEntry));
	munmap(cen, MAX_ENTRIES * sizeof(CMEntry));

	return 0;
}
