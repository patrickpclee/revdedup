/*
 * httprequest.h
 *
 *  Created on: Dec 7, 2012
 *      Author: chng
 */

#ifndef HTTPREQUEST_H_
#define HTTPREQUEST_H_

#include "rdclient.h"

typedef struct {
	char func[STRING_BUF_SIZE];
	uint32_t header_cnt;
	struct {
		char key[STRING_BUF_SIZE];
		char value[STRING_BUF_SIZE];
	} headers[HTTP_HEADER_MAX];
	uint32_t request_size;
	uint32_t response_size;
	void * request;
	char response[1048576];
} Request;

typedef struct {
	int (*post)(Request * req);
	int (*postData)(Request * req);
	Request *(*createRequest)();
	void (*destroyRequest)(Request * req);
} HTTPReqService;

HTTPReqService * getHTTPReqService();

#endif /* HTTPREQUEST_H_ */
