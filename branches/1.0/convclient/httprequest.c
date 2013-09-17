/*
 * httprequest.c
 *
 *  Created on: Dec 7, 2012
 *      Author: chng
 */

#include <curl/curl.h>
#include <stdio.h>
#include <string.h>
#include "httprequest.h"

typedef struct {
	size_t size;
	void * data;
} Response;

static inline void generateHeader(char buffer[], const char * name,
		const char * value) {
	sprintf(buffer, "X-Field-%s: %s", name, value);
}

static inline void generatePath(char buffer[], char * func) {
	sprintf(buffer, "%s:%d/%s", HOST, PORT, func);
}

static size_t writeData(char * ptr, size_t size, size_t nmemb, void * userdata) {
	Response * res = userdata;
	memcpy(res->data + res->size, ptr, size * nmemb);
	res->size += size * nmemb;
	return nmemb;
}

static int post(Request * req) {
	CURL * curl = curl_easy_init();
	CURLcode ret;
	struct curl_slist * curl_headers = NULL;
	Response res;
	char buffer[STRING_BUF_SIZE];
	int i;

	if (!curl) {
		return -1;
	}

	res.size = 0;
	res.data = req->response;

	for (i = 0; i < req->header_cnt; i++) {
		generateHeader(buffer, req->headers[i].key, req->headers[i].value);
		curl_headers = curl_slist_append(curl_headers, buffer);
	}
	curl_easy_setopt(curl, CURLOPT_POST, 1);
	generatePath(buffer, req->func);
	curl_easy_setopt(curl, CURLOPT_URL, buffer);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeData);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, 0);
	ret = curl_easy_perform(curl);

	if (ret != CURLE_OK) {
		fprintf(stderr, "Error on %s : Code %d\n", req->func, ret);
		curl_slist_free_all(curl_headers);
		curl_easy_cleanup(curl);
		return -1;
	}

	curl_slist_free_all(curl_headers);
	curl_easy_cleanup(curl);
	return 0;
}

static int postData(Request * req) {
	CURL * curl = curl_easy_init();
	CURLcode ret;
	struct curl_slist * curl_headers = NULL;
	Response res;
	char buffer[STRING_BUF_SIZE];
	int i;

	if (!curl) {
		return -1;
	}

	res.size = 0;
	res.data = req->response;

	for (i = 0; i < req->header_cnt; i++) {
		generateHeader(buffer, req->headers[i].key, req->headers[i].value);
		curl_headers = curl_slist_append(curl_headers, buffer);
	}
	curl_easy_setopt(curl, CURLOPT_POST, 1);
	generatePath(buffer, req->func);
	curl_easy_setopt(curl, CURLOPT_URL, buffer);
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, curl_headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeData);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, req->request_size);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, req->request);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &res);
	// curl_easy_setopt(curl, CURLOPT_SOCKOPTFUNCTION, sockoptfn);
	// curl_easy_setopt(curl, CURLOPT_FORBID_REUSE, 1);
	// curl_easy_setopt(curl, CURLOPT_FRESH_CONNECT, 1);

	ret = curl_easy_perform(curl);

	if (ret != CURLE_OK) {
		fprintf(stderr, "Error on %s : Code %d\n", req->func, ret);
		curl_slist_free_all(curl_headers);
		curl_easy_cleanup(curl);
		return -1;
	}

	curl_slist_free_all(curl_headers);
	curl_easy_cleanup(curl);
	return 0;
}

static Request * createRequest() {
	Request * req = malloc(sizeof(Request));
	return req;
}

static void destroyRequest(Request * req) {
	free(req);
}

static HTTPReqService service = {
		.post = post,
		.postData = postData,
		.createRequest = createRequest,
		.destroyRequest = destroyRequest,
};

HTTPReqService * getHTTPReqService() {
	return &service;
}
