#include <revdedup.h>

int main(int argc, char * argv[]){
	int i;
	int fd = open(DATA_DIR "slog", O_RDWR);
	SMEntry * sen = MMAP_FD(fd, MAX_ENTRIES(sizeof(SMEntry)));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDWR);
	BMEntry * ben = MMAP_FD(fd, MAX_ENTRIES(sizeof(BMEntry)));
	close(fd);

	SegmentLog * slog = (SegmentLog*)sen;

	//fprintf(stderr,"Number of segments: %d\n",slog->segID);
	for(i = 1;i<=slog->segID;i++)
		if (sen[i].pos > ben[sen[i].bucket].size)
			printf("Bucket %08lx(%d), segment %d(%d)\n",sen[i].bucket,ben[sen[i].bucket].size,i,sen[i].pos);

	munmap(sen,MAX_ENTRIES(sizeof(SMEntry)));


	return 0;
}
