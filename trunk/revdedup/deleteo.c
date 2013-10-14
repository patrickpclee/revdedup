/**
 * @file 	delete.c
 * @brief	Implements bucket deletion with version tagged
 * @author	Ng Chun Ho
 */

#include <revdedup.h>

int main(int argc, char * argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Usage: %s images version\n", argv[0]);
		return 0;
	}
	char buf[128];
	uint32_t insts = atoi(argv[1]);
	uint32_t version = atoi(argv[2]);
	int fd;
	uint64_t i;

	fd = open(DATA_DIR "blog", O_RDWR);
	BMEntry * ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	BucketLog * blog = (BucketLog *) ben;
	close(fd);

	fd = open(DATA_DIR "ilog", O_RDWR);
	ssize_t isize = lseek(fd, 0, SEEK_END);
	IMEntry * ien = MMAP_FD(fd, isize);
	insts = isize / sizeof(IMEntry);
	close(fd);

	/// Remove direct and indirect recipe
	for (i = 0; i < insts; i++) {
		sprintf(buf, DATA_DIR "image/%lu-%u", i, version);
		unlink(buf);
		sprintf(buf, DATA_DIR "image/i%lu-%u", i, version);
		unlink(buf);
		ien[i].old--;
		ien[i].deleted++;
	}

	/// Remove buckets tagged with specified version
	for (i = 1; i <= blog->bucketID; i++) {
		if (ben[i].ver == version) {
			sprintf(buf, DATA_DIR "bucket/%08lx", i);
			unlink(buf);
			ben[i].size = 0;
			ben[i].psize = 0;
		}
	}
	sync();

	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));
	munmap(ien, INST_MAX(sizeof(IMEntry)));
	return 0;
}
