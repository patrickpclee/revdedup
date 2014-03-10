#include <revdedup.h>

int main (){
	int fd = open(DATA_DIR "blog", O_RDWR);

	if(fd < 0){
		perror("open()");
		return -1;
	}
	BMEntry * ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	if (ben == NULL) {
		perror("mmap()");
		return -1;
	}
	BucketLog * blog = (BucketLog *) ben;
	close(fd);


	long long total = 0;
	int i;
	for (i = 1; i <= blog->bucketID; i++) {
		total += ben[i].size;
	}
	printf("%lld\n",total);
		
	return 0;
}
