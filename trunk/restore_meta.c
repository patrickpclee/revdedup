/**
 * @file 	restore.c
 * @brief	Implements restoration of images without reverse deduplication
 * @author	Ng Chun Ho
 */

#include <revdedup.h>
#include <queue.h>
#include <sys/time.h>
#include <datatable.h>
#include "minilzo.h"

/**
 * Definition of data info for all threads
 */
typedef struct {
	int ifd;					/*!< Direct recipe file descriptor */
	int ofd;					/*!< File descriptor for sending image data */
	Direct * dir;				/*!< Direct recipe entries */
	uint64_t size;				/*!< Size of direct recipe */
	uint64_t cnt;				/*!< Number of direct recipe entries */
	volatile uint64_t cur;		/*!< Direct entry currently processed */
	pthread_spinlock_t lock;	/*!< Lock for modifying cur */
} DataInfo;

typedef struct _BucketCache{
	int bid;
	struct _BucketCache* prev;
	struct _BucketCache* next;
} BucketCache;

IMEntry * ien;
SMEntry * sen;
CMEntry * cen;
BMEntry * ben;

/** Data Info for all threads */
DataInfo dinfo;
/** Data Table for holding decompressed data that will be later sending out */
DataTable * dt;
/** Queue for holding memory blocks for decompression */
Queue * dq;

BucketCache* bcache = NULL;
BucketCache* tail=NULL;
uint32_t cache_size = 0;
uint64_t seg_seeks=0; //unique segments number
float inf_cache_seeks=0; //all bucket only read once
uint64_t b1_seeks=0;  //cache size as large as one bucket
uint64_t b2_seeks=0; //cache size as large as two buckets
pthread_mutex_t seek_mutex;
struct timeval pftime;
uint64_t last_bid;
uint64_t llast_bid;

/**
 * Segment prefetching routine
 * @param ptr		Queue that holds either segment or bucket number
 */
void * prefetch(void * ptr) {
	struct timeval a,b,c;
	gettimeofday(&a,NULL);
	Queue * q = (Queue *) ptr;
	uint64_t sid, lsid = 0, bid = 0;
	uint32_t pos, len;
	char buf[128];
	uint64_t tmp_size = 0;
	pthread_mutex_lock(&seek_mutex);
	while ((sid = (uint64_t) Dequeue(q)) != 0) {
#ifdef PREFETCH_WHOLE_BUCKET
        bid = sen[sid].bucket;
        sprintf(buf, DATA_DIR "bucket/%08lx", bid);
        int fd = open(buf, O_RDONLY);
        posix_fadvise(fd, 0, 0, POSIX_FADV_WILLNEED);
        close(fd);
	}
#else
		tmp_size += sen[sid].len;
		if (bid == 0) {
			bid = sen[sid].bucket;
			pos = sen[sid].pos;
			len = sen[sid].len;
			continue;
		}
		if (sen[sid].bucket == bid && pos + len == sen[sid].pos) {
			len += sen[sid].len;
			continue;
		}

		sprintf(buf, DATA_DIR "bucket/%08lx", bid);
		int fd = open(buf, O_RDONLY);
		posix_fadvise(fd, pos, len, POSIX_FADV_WILLNEED);
		//posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
		seg_seeks++;
		if (bid != last_bid){
			b1_seeks++;
			if(bid != llast_bid){
				b2_seeks++;
				llast_bid = last_bid;
				last_bid = bid;
			}
			//fprintf(stdout,"BID: %d, size: %ld\n",last_bid,tmp_len);
		}
		/*
		if(bcache == NULL){
			BucketCache* tmp = (BucketCache*)malloc(sizeof(BucketCache));
			tmp->bid = bid;
			tmp->next=NULL;
			bcache = tmp;
		} else {
			BucketCache* tmp;
			int unique = 1;
			for(tmp=bcache;tmp!=NULL;tmp=tmp->next){
				if(tmp->bid == bid){
					unique=0;
					break;
				}
			}
			if(unique){
				tmp = (BucketCache*)malloc(sizeof(BucketCache));
				tmp->bid = bid;
				tmp->next = bcache->next;
				bcache->next = tmp;
				inf_cache_seeks++;
			}
		}
		*/
		close(fd);

		bid = sen[sid].bucket;
		pos = sen[sid].pos;
		len = sen[sid].len;

	}
	sprintf(buf, DATA_DIR "bucket/%08lx", bid);
	int fd = open(buf, O_RDONLY);
	posix_fadvise(fd, pos, len, POSIX_FADV_WILLNEED);
	//posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
	seg_seeks++;
	if (bid != last_bid){
		b1_seeks++;
		if(bid != llast_bid){
			b2_seeks++;
			llast_bid = last_bid;
			last_bid = bid;
		}
		//fprintf(stdout,"BID: %d, size: %ld\n",last_bid,tmp_len);
	}
	/*
 	if(bcache == NULL){
			BucketCache* tmp = (BucketCache*)malloc(sizeof(BucketCache));
			tmp->bid = bid;
			tmp->next=NULL;
			bcache = tmp;
		} else {
			BucketCache* tmp;
			int unique = 1;
			for(tmp=bcache;tmp!=NULL;tmp=tmp->next){
				if(tmp->bid == bid){
					unique=0;
					break;
				}
			}
			if(unique){
				tmp = (BucketCache*)malloc(sizeof(BucketCache));
				tmp->bid = bid;
				tmp->next = bcache->next;
				bcache->next = tmp;
				inf_cache_seeks++;
			}
	}
	*/
	close(fd);
#endif
	gettimeofday(&b,NULL);
	timersub(&b,&a,&c);
	pftime.tv_sec += c.tv_sec + (int)(c.tv_usec+pftime.tv_usec)/1000000;
	pftime.tv_usec = (c.tv_usec+pftime.tv_usec)%1000000;
	pthread_mutex_unlock(&seek_mutex);
	/*
	BucketCache* tmp;
	for(tmp=bcache;tmp!=NULL;tmp=bcache){
		bcache = bcache->next;
		free(tmp);
	}
	*/
	return NULL;
}

/**
 * Decompression routine
 * @param ptr		useless
 */
void * decompress(void * ptr) {
	uint8_t * cdata = MMAP_MM(MAX_SEG_SIZE);
	SMEntry * en;
	DataEntry * den;
	uint64_t size, cur;
	int32_t fd;
	char buf[128];
	uint64_t tmp;
	uint64_t prev_bid;

	while (1) {
		pthread_spin_lock(&dinfo.lock);
		cur = dinfo.cur++;
		pthread_spin_unlock(&dinfo.lock);
		if (unlikely(cur >= dinfo.cnt)) {
			break;
		}
		/// If it is locked, then other threads have decompressed this segment
		if (pthread_spin_trylock(&dt->en[dinfo.dir[cur].id].lock)) {
			continue;
		}
		en = &sen[dinfo.dir[cur].id];
		den = &dt->en[dinfo.dir[cur].id];
		den->data = Dequeue(dq);
		sprintf(buf, DATA_DIR "bucket/%08lx", en->bucket);
		fd = open(buf, O_RDONLY);
#ifdef DISABLE_COMPRESSION
		//assert(pread(fd, den->data, en->len, en->pos) == en->len);
		tmp = pread(fd, den->data, en->len, en->pos);
		den->size = tmp;
#else
		if (sen->compressed) {
			assert(pread(fd, cdata, en->len, en->pos) == en->len);
			lzo1x_decompress(cdata, en->len, den->data, &size, NULL);
			den->size = size;
		} else {
			assert(pread(fd, den->data, en->len, en->pos) == en->len);
			den->size = en->len;
		}
#endif
		close(fd);
		pthread_mutex_unlock(&den->mutex);
	}

	munmap(cdata, MAX_SEG_SIZE);
	return NULL;
}

/**
 * Routine for sending out data
 * @param ptr	useless
 */
void * send(void * ptr) {
	//int start = (int)ptr;
	uint8_t * zero = MMAP_MM(ZERO_SIZE);
	SMEntry * en;
	DataEntry * den;
	uint64_t i, j;
	for (i = 0; i < dinfo.cnt; i++) {
		en = &sen[dinfo.dir[i].id];
		den = &dt->en[dinfo.dir[i].id];

		/// Ensure that the segment is fully decompressed
		pthread_mutex_lock(&den->mutex);
		pthread_mutex_unlock(&den->mutex);

		for (j = 0; j < en->chunks; j++) {
			CMEntry * cptr = &cen[en->cid + j];
			if (cptr->len == 0) {
				assert(write(dinfo.ofd, zero, ZERO_SIZE));
				continue;
			}
			assert(write(dinfo.ofd, den->data + cptr->pos, cptr->len) == cptr->len);
		}

		if (--den->cnt == 0) {
			Enqueue(dq, den->data);
		}
	}
	munmap(zero, ZERO_SIZE);
	return NULL;
}

int main(int argc, char * argv[]) {
	if (argc != 4) {
		fprintf(stderr, "Usage: %s instance version file\n", argv[0]);
		return 0;
	}
	char buf[128];
	uint32_t i;
	int32_t fd;
	FILE* lfd;

	lfd = fopen("bucket_seeks.log","a");
	memset(&pftime,0,sizeof(struct timeval));
	pthread_mutex_init(&seek_mutex,NULL);

	uint32_t ins = atoi(argv[1]);
	uint32_t ver = atoi(argv[2]);
	/// Check whether the image is reversely deduplicated
	sprintf(buf, DATA_DIR "image/i%u-%u", ins, ver);
	if (access(buf, F_OK) == 0) {
		fprintf(stderr, "This version is get revdeduped, try using restoreo\n");
		return 0;
	}

	/// Retrieving Metadata
	fd = open(DATA_DIR "ilog", O_RDONLY);
	ien = MMAP_FD_RO(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	fd = open(DATA_DIR "slog", O_RDONLY);
	sen = MMAP_FD_RO(fd, MAX_ENTRIES(sizeof(SMEntry)));
	close(fd);

	fd = open(DATA_DIR "clog", O_RDONLY);
	cen = MMAP_FD_RO(fd, MAX_ENTRIES(sizeof(CMEntry)));
	close(fd);

	fd = open(DATA_DIR "blog", O_RDONLY);
	ben = MMAP_FD_RO(fd, MAX_ENTRIES(sizeof(BMEntry)));
	close(fd);

	/// Setup DataInfo
	sprintf(buf, DATA_DIR "image/%u-%u", ins, ver);
	dinfo.ifd = open(buf, O_RDONLY);
	dinfo.ofd = creat(argv[3], 0644);
	dinfo.size = lseek(dinfo.ifd, 0, SEEK_END);
	dinfo.dir = MMAP_FD_RO(dinfo.ifd, dinfo.size);
	dinfo.cnt = dinfo.size / sizeof(Direct);
	dinfo.cur = 0;
	pthread_spin_init(&dinfo.lock, PTHREAD_PROCESS_SHARED);

	/* Setup memory buffer for decompression
	void * data = MMAP_MM(LONGQUEUE_LENGTH * MAX_SEG_SIZE);
	dq = LongQueue();
	for (i = 0; i < LONGQUEUE_LENGTH; i++) {
		Enqueue(dq, data + i * MAX_SEG_SIZE);
	}
	*/

	/// Setup DataTable and prefetch
	dt = NewDataTable(((SegmentLog *)sen)->segID + 1);
	Queue * pfq = SuperQueue();
	pthread_t pft;
	//for (i=0;i<PF_CNT;i++){
		pthread_create(&pft, NULL, prefetch, pfq);
	//}
	uint64_t tmp_size = 0;
	for (i = 0; i < dinfo.cnt; i++) {
		DataEntry * den = &dt->en[dinfo.dir[i].id];
		if (++den->cnt == 1) {
			pthread_spin_init(&den->lock, PTHREAD_PROCESS_SHARED);
			pthread_mutex_lock(&den->mutex);
			Enqueue(pfq, (void *) dinfo.dir[i].id);
		}
		if(dinfo.dir[i].id != 0){
		int bid = sen[dinfo.dir[i].id].bucket;
		tmp_size += sen[dinfo.dir[i].id].len;
		if(bcache == NULL){
			BucketCache* tmp = (BucketCache*)malloc(sizeof(BucketCache));
			tmp->bid = bid;
			tmp->next=NULL;
			tmp->prev=NULL;
			bcache = tmp;
			tail = tmp;
			cache_size++;
		} else {
			BucketCache* tmp;
			int unique = 1;
			for(tmp=bcache;tmp!=NULL;tmp=tmp->next){
				if(tmp->bid == bid){
					unique=0;
					if(tmp != bcache){
						if(tmp == tail){
							tail = tmp->prev;
						} else {
							tmp->next->prev = tmp->prev;
						}
						tmp->prev->next = tmp->next;
						tmp->next = bcache;
						tmp->prev = NULL;
						bcache->prev = tmp;
						bcache = tmp;
					}
					break;
				}
			}
			if(unique){
				tmp = (BucketCache*)malloc(sizeof(BucketCache));
				tmp->bid = bid;
				tmp->next = bcache;
				tmp->prev = NULL;
				bcache->prev = tmp;
				bcache = tmp;
				cache_size++;
				if(cache_size > CACHE_BUCKETS){
					tail = tail->prev;
					free(tail->next);
					tail->next = NULL;
					cache_size--;
				}
				inf_cache_seeks++;
			}
	}
		}
	}
	inf_cache_seeks /= (tmp_size/(1024*1024));
	Enqueue(pfq, NULL);

	// remove decompress and send
	/* Setup decompress and send
	pthread_t dct[DPS_CNT];
	pthread_t sdt;
	for (i = 0; i < DPS_CNT; i++) {
		pthread_create(dct + i, NULL, decompress, NULL);
	}
	pthread_create(&sdt, NULL, send, NULL);

	/// wait for the outstanding threads
	for (i = 0; i < DPS_CNT; i++) {
		pthread_join(dct[i], NULL);
	}
	//for (i = 0; i < SEND_THREAD_CNT; i++) {
	//	pthread_join(sdt[i], NULL);
	//}

	pthread_join(sdt, NULL);
	*/
	//pthread_cancel(pft);
	pthread_join(pft,NULL);
	
	pthread_mutex_lock(&seek_mutex);
	fprintf(stdout,"Inst: %d, Ver: %d, SegNum: %ld, SegSeeks: %ld, InfBucSeeks: %.4f, 1BSeeks: %ld, 2BSeeks: %ld\n", ins, ver, dinfo.cnt, seg_seeks,inf_cache_seeks,b1_seeks,b1_seeks);
	pthread_mutex_unlock(&seek_mutex);
	DelQueue(pfq);
	//DelQueue(dq);
	//munmap(data, LONGQUEUE_LENGTH * MAX_SEG_SIZE);

	DelDataTable(dt);

	munmap(ien, INST_MAX(sizeof(IMEntry)));
	munmap(sen, MAX_ENTRIES(sizeof(SMEntry)));
	munmap(cen, MAX_ENTRIES(sizeof(CMEntry)));
	munmap(ben, MAX_ENTRIES(sizeof(BMEntry)));
	munmap(dinfo.dir, dinfo.size);
	close(dinfo.ifd);
	close(dinfo.ofd);
	return 0;
}
