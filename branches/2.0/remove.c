/**
 * @file 	remove.c
 * @brief	Remove images without reverse deduplication
 * @author	Ng Chun Ho
 */

#include <revdedup.h>

SMEntry * sen;
BMEntry * ben;

int main(int argc, char * argv[]) {
	if (argc != 2) {
		fprintf(stderr, "Usage : %s image version\n", argv[0]);
		return 0;
	}
	char buf[128];
	uint32_t ins = atoi(argv[1]);
	uint32_t ver = atoi(argv[2]);
	uint32_t fd, ifd;
	fd = open(DATA_DIR "slog", O_RDWR);
	sen = MMAP_FD(fd, MAX_ENTRIES(sizeof(SMEntry)));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDWR);
	ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	close(fd);

	sprintf(buf, DATA_DIR "image/%u-%u", ins, ver);
	ifd = open(buf, O_RDONLY);

	Direct dir;
	while (read(fd, &dir, sizeof(dir)) > 0) {
		/// Decrements reference count of each direct reference
		if (--sen[dir.id].ref == 0) {
			ben[sen[dir.id].bucket].psize += sen[dir.id].len;
		}
	}
	close(ifd);
	unlink(buf);

	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));
	munmap(sen, MAX_ENTRIES(sizeof(SMEntry)));

	return 0;
}
