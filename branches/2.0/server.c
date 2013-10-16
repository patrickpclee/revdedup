/**
 * @file 	server.c
 * @brief	Implements http server for RevDedup
 * @author	Ng Chun Ho
 */

#include <microhttpd.h>
#include <revdedup.h>
#include "index.h"
#include "image.h"
#include "compress.h"
#include "bucket.h"

#define CH(x) ((char *)x)
#define SZ_U	sizeof(uint64_t)
#define SZ_S	sizeof(Segment)

IMEntry * ien;
Queue * mmq;

Queue * miq;
Queue * imq;

Queue * dcq;
Queue * bdq;

typedef struct {
    size_t size;
    size_t ptr;
	void * data;
} Request;

/**
 * Gathers HTTP Request Data
 * @param req		Request
 * @param data		Pointer to data
 * @param size		Data size
 * @return			Request
 */
Request * buildRequest(Request * req, const void * data, size_t size) {
	if (req == NULL) {
		req = malloc(sizeof(Request));
		req->size = META_THREAD_MEM;
		req->ptr = 0;
		req->data = malloc(req->size);
	}

	while (req->ptr + size > req->size) {
		req->size *= 4;
		req->data = realloc(req->data, req->size);
	}
	memcpy(req->data + req->ptr, data, size);
	req->ptr += size;

	return req;
}

/**
 * Implements read function for sending out data in microhttpd
 * @param cls		Custom argument
 * @param pos		Position to read
 * @param buf		Buffer to fill
 * @param max		Bytes to read
 * @return
 */
static ssize_t readFn(void *cls, uint64_t pos, char *buf, size_t max) {
	return read(fileno((FILE *) cls), buf, max);
}

/**
 * Implements free function after read in microhttpd
 * @param cls		Custom argument
 */
static void freeFn(void *cls) {
	pclose((FILE *) cls);
}

/**
 * Reply client with no HTTP Response Body
 * @param conn		HTTP Connection
 * @param code		HTTP status code
 * @return			MHD_YES for microhttpd
 */
static int replyNone(struct MHD_Connection * conn, int code) {
	struct MHD_Response * resp = MHD_create_response_from_buffer(0, "",
			MHD_RESPMEM_PERSISTENT);
	MHD_queue_response(conn, code, resp);
	MHD_destroy_response(resp);
	return MHD_YES;
}

/**
 * Routine for sending out data through HTTP.
 * It will be called multiple times when the request has a body.
 * Copy the body to temporary location, and save its pointer to con_cls
 * @param cls				Custom argument
 * @param conn				HTTP Connection
 * @param url				HTTP request URL
 * @param method			HTTP request method
 * @param version			HTTP request version
 * @param upload_data		HTTP request body (partial)
 * @param upload_data_size	HTTP request body size (partial)
 * @param con_cls			Pointer for holding temporary data
 * @return					MHD_YES for microhttpd
 */
static int processSend(void *cls, struct MHD_Connection *conn, const char *url,
		const char *method, const char *version, const char *upload_data,
		size_t *upload_data_size, void **con_cls) {
	uint8_t buf[128];
	uint32_t inst, ver;
	FILE * filep;
	if (sscanf(url, "/%u/%u", &inst, &ver) == EOF) {
		return replyNone(conn, MHD_HTTP_BAD_REQUEST);
	}
	sprintf(buf, DATA_DIR "image/%u-%u", inst, ver);
	if (access(buf, R_OK) == -1) {
		return replyNone(conn, MHD_HTTP_BAD_REQUEST);
	}

	int fd = open(DATA_DIR "ilog", O_RDONLY);
	IMEntry * ien = MMAP_FD_RO(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	sprintf(buf, DATA_DIR "image/i%u-%u", inst, ver);
	if (access(buf, R_OK) == -1) {
		sprintf(buf, "./restore %u %u /dev/stdout", inst, ver);
	} else {
		sprintf(buf, "./restoreo %u %u /dev/stdout", inst, ver);
	}
	filep = popen(buf, "r");
	struct MHD_Response * resp = MHD_create_response_from_callback(
			ien[inst].vers[ver].size, 1048576, readFn, filep, freeFn);
    MHD_queue_response(conn, MHD_HTTP_OK, resp);
    MHD_destroy_response(resp);
    munmap(ien, INST_MAX(sizeof(IMEntry)));
	return MHD_YES;
}

/**
 * Routine for processing upload metadata through HTTP
 * It will be called multiple times when the request has a body.
 * Copy the body to temporary location, and save its pointer to con_cls
 * @param cls				Custom argument
 * @param conn				HTTP Connection
 * @param url				HTTP request URL
 * @param method			HTTP request method
 * @param version			HTTP request version
 * @param upload_data		HTTP request body (partial)
 * @param upload_data_size	HTTP request body size (partial)
 * @param con_cls			Pointer for holding temporary data
 * @return					MHD_YES for microhttpd
 */
static int processMeta(void *cls, struct MHD_Connection *conn, const char *url,
		const char *method, const char *version, const char *upload_data,
		size_t *upload_data_size, void **con_cls) {
	uint8_t buf[128];
	uint32_t inst, ver;
	uint64_t i;

	if (!strcmp(url, "/sync")) {
		sync();
		return replyNone(conn, MHD_HTTP_OK);
	}

	Request * req = *(Request **) con_cls;
	if (req == NULL || *upload_data_size) {
		req = buildRequest(req, upload_data, *upload_data_size);
		*con_cls = req;
		*upload_data_size = 0;
		return MHD_YES;
	}
	*con_cls = NULL;

	if (strcmp(method, "POST") || sscanf(url, "/%u", &inst) == EOF) {
		free(req->data);
		free(req);
		return replyNone(conn, MHD_HTTP_BAD_REQUEST);
	}
	ver = ien[inst].versions++;
	ien[inst].recent++;

	uint64_t segcnt = req->ptr / sizeof(Segment);
	Segment * segs = req->data;

	for (i = 0; i < segcnt; i++) {
		Enqueue(miq, &segs[i]);
	}
	/** Construct response */
	sprintf(buf, DATA_DIR "image/%u-%u", inst, ver);
	int fd = creat(buf, 0644);
	uint64_t * resp_c = malloc(segcnt * sizeof(uint64_t));
	for (i = 0; i < segcnt; i++) {
		Dequeue(imq);
		if (segs[i].unique) {
			resp_c[i] = segs[i].id;
			ien[inst].vers[ver].space += segs[i].len;
			GetIndexService()->putSegment(&segs[i], 0);
		} else {
			resp_c[i] = 0;
		}
		ien[inst].vers[ver].size += segs[i].len;
		assert(write(fd, &segs[i], sizeof(Direct)) == sizeof(Direct));
	}
	close(fd);

	struct MHD_Response * resp = MHD_create_response_from_buffer(
			segcnt * sizeof(uint64_t), resp_c, MHD_RESPMEM_MUST_FREE);
	MHD_queue_response(conn, MHD_HTTP_OK, resp);
	MHD_destroy_response(resp);

	free(req->data);
	free(req);
	return MHD_YES;
}

/**
 * Routine for processing upload data through HTTP
 * It will be called multiple times when the request has a body.
 * Copy the body to temporary location, and save its pointer to con_cls
 * @param cls				Custom argument
 * @param conn				HTTP Connection
 * @param url				HTTP request URL
 * @param method			HTTP request method
 * @param version			HTTP request version
 * @param upload_data		HTTP request body (partial)
 * @param upload_data_size	HTTP request body size (partial)
 * @param con_cls			Pointer for holding temporary data
 * @return					MHD_YES for microhttpd
 */
static int processData(void *cls, struct MHD_Connection *conn, const char *url,
		const char *method, const char *version, const char *upload_data,
		size_t *upload_data_size, void **con_cls) {

	Request * req = *(Request **) con_cls;
	if (req == NULL || *upload_data_size) {
		req = buildRequest(req, upload_data, *upload_data_size);
		*con_cls = req;
		*upload_data_size = 0;
		return MHD_YES;
	}
	*con_cls = NULL;

	uint64_t sid, size;
	if (strcmp(method, "POST") || sscanf(url, "/%lu", &sid) == EOF) {
		free(req->data);
		free(req);
		return replyNone(conn, MHD_HTTP_BAD_REQUEST);
	}
	Segment * seg = malloc(sizeof(Segment));
	seg->id = sid;
	seg->unique = 1;
	GetIndexService()->getSegment(seg);
	seg->data = req->data;
	seg->cdata = Dequeue(mmq);
	Enqueue(dcq, seg);

	free(req);
	return replyNone(conn, MHD_HTTP_OK);;
}

/**
 * Routine to destroy segment after it is fully processed
 * @param ptr		useless
 */
void * end(void * ptr) {
	Segment * seg;
	while ((seg = (Segment *)Dequeue(bdq)) != NULL) {
		Enqueue(mmq, seg->cdata);
		free(seg->data);
		free(seg);
	}
	return NULL;
}

/** Used to catch SIGINT */
void sighandler(int signal) {
    return;
}


int main(void) {
	signal(SIGINT, sighandler);
	uint64_t i;
	int fd = open(DATA_DIR "ilog", O_RDWR | O_CREAT, 0644);
	assert(!ftruncate(fd, INST_MAX(sizeof(IMEntry))));
	ien = MMAP_FD(fd, INST_MAX(sizeof(IMEntry)));
	close(fd);

	mmq = LongQueue();
	void * data = MMAP_MM(LONGQUEUE_LENGTH * MAX_SEG_SIZE);
	for (i = 0; i < LONGQUEUE_LENGTH; i++) {
		Enqueue(mmq, data + i * MAX_SEG_SIZE);
	}

	IndexService * is = GetIndexService();
	miq = SuperQueue();
	imq = SuperQueue();
	is->start(miq, imq);

	CompressService * cs = GetCompressService();
	BucketService * bs = GetBucketService();
	dcq = NewQueue();
	bdq = NewQueue();
	Queue * cbq = NewQueue();
	cs->start(dcq, cbq);
	bs->start(cbq, bdq);

	pthread_t endt;
	pthread_create(&endt, NULL, end, NULL);

	/// Start servers at 3 ports
	struct MHD_Daemon * sendd = MHD_start_daemon(
			MHD_USE_SELECT_INTERNALLY | MHD_USE_DEBUG, SEND_PORT,
	            NULL, NULL, processSend, NULL,
	            MHD_OPTION_THREAD_POOL_SIZE, SEND_THREAD_CNT,
	            MHD_OPTION_CONNECTION_MEMORY_LIMIT, SEND_THREAD_MEM,
	            MHD_OPTION_END);

	struct MHD_Daemon * metad = MHD_start_daemon(
			MHD_USE_SELECT_INTERNALLY | MHD_USE_DEBUG, META_PORT,
	            NULL, NULL, processMeta, NULL,
	            MHD_OPTION_THREAD_POOL_SIZE, META_THREAD_CNT,
	            MHD_OPTION_CONNECTION_MEMORY_LIMIT, META_THREAD_MEM,
	            MHD_OPTION_END);

	struct MHD_Daemon * datad = MHD_start_daemon(
			MHD_USE_SELECT_INTERNALLY | MHD_USE_DEBUG, DATA_PORT,
	            NULL, NULL, processData, NULL,
	            MHD_OPTION_THREAD_POOL_SIZE, DATA_THREAD_CNT,
	            MHD_OPTION_CONNECTION_MEMORY_LIMIT, DATA_THREAD_MEM,
	            MHD_OPTION_END);

	pause();

	MHD_stop_daemon(datad);
	MHD_stop_daemon(metad);
	MHD_stop_daemon(sendd);

	Enqueue(dcq, NULL);
	cs->stop();
	bs->stop();
	pthread_join(endt, NULL);

	DelQueue(dcq);
	DelQueue(bdq);

	Enqueue(miq, NULL);
	is->stop();
	DelQueue(miq);
	DelQueue(imq);

	munmap(data, LONGQUEUE_LENGTH * MAX_SEG_SIZE);
	munmap(ien, INST_MAX(sizeof(IMEntry)));
	return 0;
}
