/*
 * httpservice.c
 *
 *  Created on: 30 Jun, 2012
 *      Author: chng
 */

#include <jansson.h>
#include "rdserver.h"
#include "fingerprint.h"
#include "httpservice.h"
#include "rdservice.h"


#define THREAD_MEMORY (16 << 20)	// 64 MB
#define THREAD_POOL_SIZE 24
#define REQUEST_DEFAULT_SIZE	(8 << 20)	// 16MB

typedef struct MHD_Connection Connection;

static HTTPService service;

typedef struct {
	size_t size;
	size_t ptr;
	void * data;
} Request;

static Request * createRequest() {
	Request * req = malloc(sizeof(Request));
	req->size = REQUEST_DEFAULT_SIZE;
	req->ptr = 0;
	req->data = malloc(REQUEST_DEFAULT_SIZE);
	memset(req->data, 0, REQUEST_DEFAULT_SIZE);
	return req;
}

static void destroyRequest(Request * req) {
	free(req->data);
	free(req);
}

static const char * getHeader(Connection * connection, const char * field_name) {
	char header[STRING_BUF_SIZE];
	strcpy(header, "X-Field-");
	strncpy(header + strlen("X-Field-"), field_name,
			STRING_BUF_SIZE - strlen("X-Field-"));
	return MHD_lookup_connection_value(connection, MHD_HEADER_KIND, header);
}

static int replyOK_free(Connection * connection, char * resp) {
	struct MHD_Response * response = MHD_create_response_from_buffer(
			strlen(resp), resp, MHD_RESPMEM_MUST_FREE);
	int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
	MHD_destroy_response(response);
	return ret;
}

static int replyOK(Connection * connection, char * resp) {
	struct MHD_Response * response = MHD_create_response_from_buffer(
			strlen(resp), resp, MHD_RESPMEM_MUST_COPY);
	int ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
	MHD_destroy_response(response);
	return ret;
}

static int replyBadRequest(Connection * connection) {
	struct MHD_Response * response = MHD_create_response_from_buffer(
			0, "", MHD_RESPMEM_PERSISTENT);
	int ret = MHD_queue_response(connection, MHD_HTTP_BAD_REQUEST, response);
	MHD_destroy_response(response);
	return ret;
}

static ssize_t readContentFn(void *cls, uint64_t pos, char *buf, size_t max) {
	int fd = *(int *) cls;
	ssize_t ret = read(fd, buf, max);
	return ret;
}

static void readContentFreeFn(void *cls) {
	close(*(int *)cls);
	free(cls);
}


static int dispatch(void *cls, struct MHD_Connection *connection,
        const char *url,
        const char *method, const char *version,
        const char *upload_data,
        size_t *upload_data_size, void **con_cls) {

	TDService * tds = getTDService();
	int i;

	// Get the request body
	Request * req = *(Request **)con_cls;
	if (req == NULL) {
		req = createRequest();
		*con_cls = req;
		return MHD_YES;
	}
	if (*upload_data_size) {
		while (req->ptr + *upload_data_size > req->size) {
			req->size *= 2;
			req->data = realloc(req->data, req->size);
		}
		memcpy(req->data + req->ptr, upload_data, *upload_data_size);
		req->ptr += *upload_data_size;
		*upload_data_size = 0;
		return MHD_YES;
	}
	*con_cls = NULL;

	char * reply = "";
	json_error_t err;
	// Dispatch the request
	int ret;
	if (METHOD_GET(method)) {
		uint64_t size;
		int * pipefds;
		if (IS_HTTP_FUNC(url, "/image/")) {
			// Create a pipe
			pipefds = malloc(2 * sizeof(int));
			ret = pipe(pipefds);
			if (ret == -1) {
				perror(__FUNCTION__);
			}
			ret = tds->getImage(url + strlen("/image/"), pipefds[1], &size);
			if (ret == -1)
				goto reply_bad;

		} else if (IS_HTTP_FUNC(url, "/imageversion/")) {
			// Create a pipe
			char name[STRING_BUF_SIZE];
			uint64_t ver;
			sscanf(url, "/imageversion/%[^/]/%ld", name, &ver);
			printf("Get ImageVersion %s %ld\n", name, ver);
			pipefds = malloc(2 * sizeof(int));
			ret = pipe(pipefds);
			if (ret == -1) {
				perror(__FUNCTION__);
			}

			ret = tds->getImageVersion(name, pipefds[1], ver, &size);
			if (ret == -1) {
				printf("Bad Request\n");
				goto reply_bad;
			}
		} else {
			goto reply_bad;
		}
		// Manually create response
		struct MHD_Response * response = MHD_create_response_from_callback(size,
				1048576, readContentFn, pipefds, readContentFreeFn);
		MHD_queue_response(connection, MHD_HTTP_OK, response);
		MHD_destroy_response(response);
		destroyRequest(req);
		return MHD_YES;
	}
	if (METHOD_POST(method)) {
		if (IS_HTTP_FUNC(url, "/segmentexist")) {
			// Check Chunk Exist
			printf("Check Segment Existence\n");
			const char * name = getHeader(connection, "Name");
			json_t * fps_json = json_loads(req->data, 0, &err);
			json_t * exist_json = json_array();
			uint32_t cnt = json_array_size(fps_json);
			Fingerprint * fps = malloc(cnt * sizeof(Fingerprint));
			char * exist = malloc(cnt * sizeof(char));
			for (i = 0; i < cnt; i++) {
				hex2bin(json_string_value(json_array_get(fps_json, i)), &fps[i]);
			}

			ret = tds->segmentExist(name, cnt, fps, exist);

			for (i = 0; i < cnt; i++) {
				json_array_append_new(exist_json, json_integer(exist[i]));
			}
			reply = json_dumps(exist_json, JSON_PRESERVE_ORDER);
			free(exist);
			free(fps);
			json_decref(exist_json);
			json_decref(fps_json);

			goto reply_ok_free;
		} else if (IS_HTTP_FUNC(url, "/segmentfp")) {
			// Submit Segment FP
			const char * tmpfp = getHeader(connection, "SegmentFP");
			// printf("Put Segment FP %s\n", tmpfp);
			Fingerprint segmentFP;
			uint32_t cnt = req->ptr / FP_SIZE;
			Fingerprint * fps = req->data;
			hex2bin(tmpfp, &segmentFP);

			ret = tds->putSegmentFP(&segmentFP, cnt, fps);

		} else if (IS_HTTP_FUNC(url, "/segment")) {
			// Submit Segment
			// printf("Put Segment\n");
			Fingerprint segmentFP;
			hex2bin(getHeader(connection, "SegmentFP"), &segmentFP);

			ret = tds->putSegmentData(&segmentFP, req->ptr, req->data);

			if (ret)
				goto reply_bad;
		} else if (IS_HTTP_FUNC(url, "/image")) {
			// Submit Image
			printf("Put Image\n");
			const char * name = getHeader(connection, "Name");
			uint64_t size = atol(getHeader(connection, "Size"));
			json_t * fps_json = json_loads(req->data, 0, &err);
			uint32_t cnt = json_array_size(fps_json);
			Fingerprint * fps = malloc(cnt * sizeof(Fingerprint));

			for (i = 0; i < cnt; i++) {
				hex2bin(json_string_value(json_array_get(fps_json, i)), &fps[i]);
			}
			ret = tds->putImage(name, size, cnt, fps);

			free(fps);
			json_decref(fps_json);

			if (ret)
				goto reply_bad;
		} else {
			goto reply_bad;
		}
	}
	destroyRequest(req);
	return replyOK(connection, reply);

reply_ok_free:
	destroyRequest(req);
	return replyOK_free(connection, reply);

reply_bad:
	destroyRequest(req);
	return replyBadRequest(connection);
}

static void start(int port) {
	service.daemon = MHD_start_daemon(
			MHD_USE_SELECT_INTERNALLY | MHD_USE_DEBUG, port,
			NULL, NULL,
			service.dispatch, NULL,
			MHD_OPTION_THREAD_POOL_SIZE, THREAD_POOL_SIZE,
			MHD_OPTION_CONNECTION_MEMORY_LIMIT, THREAD_MEMORY,
			MHD_OPTION_END);
}

static void stop() {
	MHD_stop_daemon(service.daemon);
}


static HTTPService service = {
		.start = start,
		.stop = stop,
		.dispatch = dispatch,
};

HTTPService * getHTTPService() {
	return &service;
}
