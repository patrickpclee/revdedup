/*
 * tdclient.c
 *
 *  Created on: Dec 7, 2012
 *      Author: chng
 */

#include "tdclient.h"
#include "httprequest.h"
#include <jansson.h>
#include <stdio.h>

static SegmentService service;

static void segmentExist(uint32_t seg_cnt, Segment * segs) {
	HTTPReqService * hrs = getHTTPReqService();
	Request * req = hrs->createRequest();
	json_t * req_json = json_array();
	json_t * res_json;
	json_error_t err;
	int i;

	strcpy(req->func, "segmentexist");
	req->header_cnt = 0;

	for (i = 0; i < seg_cnt; i++) {
		json_array_append_new(req_json, json_string(segs[i].segmentfp));
	}
	req->request = json_dumps(req_json, JSON_PRESERVE_ORDER);
	req->request_size = strlen(req->request) + 1;
	hrs->postData(req);
	res_json = json_loads(req->response, 0, &err);
	for (i = 0; i < seg_cnt; i++) {
		segs[i].exist = json_integer_value(json_array_get(res_json, i));
	}
	json_decref(res_json);
	free(req->request);
	json_decref(req_json);
	hrs->destroyRequest(req);
}

static void * putSegment(void * ptr) {
	Segment * seg;
	HTTPReqService * hrs = getHTTPReqService();
	while ((seg = queue_pop(service.in)) != NULL) {
		Request * req = hrs->createRequest();
		strcpy(req->func, "segment");
		req->header_cnt = 1;
		strcpy(req->headers[0].key, "SegmentFP");
		strcpy(req->headers[0].value, seg->segmentfp);
		req->request_size = seg->blocks * BLOCK_SIZE;
		req->request = seg->data;
		hrs->postData(req);
		hrs->destroyRequest(req);
	}
	return NULL ;
}

static void putImage(Image * img) {
	HTTPReqService * hrs = getHTTPReqService();
	Request * req = hrs->createRequest();
	json_t * req_json = json_array();
	int i;
	
	printf("Put Image\n");

	strcpy(req->func, "image");
	req->header_cnt = 2;
	strcpy(req->headers[0].key, "Name");
	strcpy(req->headers[0].value, img->name);
	strcpy(req->headers[1].key, "Size");
	sprintf(req->headers[1].value, "%lu", img->size);
	for (i = 0; i < img->segcnt; i++) {
		json_array_append_new(req_json, json_string(img->segments[i].segmentfp));
	}
	
	char * req_str = json_dumps(req_json, JSON_PRESERVE_ORDER);
	req->request = req_str;
	req->request_size = strlen(req->request);
	
	hrs->postData(req);

	free(req->request);
	json_decref(req_json);
	hrs->destroyRequest(req);
}

static void start(Queue * in, Queue * out) {
	int i;
	service.threads = malloc(PUTSEG_THREAD_CNT * sizeof(pthread_t));
	service.in = in;
	service.out = out;
	for (i = 0; i < PUTSEG_THREAD_CNT; i++) {
		pthread_create(&service.threads[i], NULL, putSegment, NULL);
	}
}

static void stop() {
	int i;
	for (i = 0; i < PUTSEG_THREAD_CNT; i++) {
		queue_push(NULL, service.in);
	}
	for (i = 0; i < PUTSEG_THREAD_CNT; i++) {
		pthread_join(service.threads[i], NULL);
	}
}

static SegmentService service = {
		.start = start,
		.stop = stop,
		.segmentExist = segmentExist,
		.putImage = putImage,
};

SegmentService * getSegmentService() {
	return &service;
}


