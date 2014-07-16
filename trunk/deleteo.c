/**
 * @file 	delete.c
 * @brief	Implements bucket deletion with version tagged earlier
 * @author	Ng Chun Ho
 */

#include <sys/time.h>
#include <revdedup.h>

int main(int argc, char * argv[]) {
	if (argc != 3) {
		fprintf(stderr, "Usage : %s <number of instances> <version number>\n", argv[0]);
		return 0;
	}
	char buf[128];
	uint32_t insts = atoi(argv[1]) + 1;
	uint32_t version = atoi(argv[2]);
	int fd;
	uint64_t i;
	uint64_t dsize=0;

	fd = open(DATA_DIR "blog", O_RDWR);
	BMEntry * ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	BucketLog * blog = (BucketLog *) ben;
	close(fd);

	fd = open(DATA_DIR "ilog", O_RDWR);
	ssize_t isize = lseek(fd, 0, SEEK_END);
	IMEntry * ien = MMAP_FD(fd, isize);
	insts = isize / sizeof(IMEntry);
	close(fd);

	struct timeval x;
	TIMERSTART(x);

	uint64_t j;
	int ret;
	for (j = 0; j <= version; j++) {
	//for (j = version; j <= version; j++) {
	/// Remove direct and indirect recipe
		for (i = 0; i < insts; i++) {
			sprintf(buf, DATA_DIR "image/%lu-%u", i, j);
			ret = unlink(buf);
			if(ret == 0) {
				sprintf(buf, DATA_DIR "image/i%lu-%u", i, j);
				unlink(buf);
				ien[i].old--;
				ien[i].deleted++;
			} else
				break;
		}
	}

	/// Remove buckets tagged with earlier verions
	for (i = 1; i <= blog->bucketID; i++) {
		//if ((ben[i].size > 0) && (ben[i].ver <= version)) {
		if ((ben[i].size > 0) && (ben[i].ver == version)) {
			dsize += ben[i].size;
			sprintf(buf, DATA_DIR "bucket/%08lx", i);
			unlink(buf);
			ben[i].size = 0;
			ben[i].psize = 0;
		}
	}
	sync();
	TIMERSTOP(x);
	printf("%ld.%06ld\n", x.tv_sec, x.tv_usec);
	printf("Delete Size: %ld\n",dsize);
	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));
	munmap(ien, INST_MAX(sizeof(IMEntry)));
	return 0;
}
