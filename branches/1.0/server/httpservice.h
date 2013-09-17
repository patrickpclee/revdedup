/*
 * httpservice.h
 *
 *  Created on: 30 Jun, 2012
 *      Author: chng
 */

#ifndef HTTPSERVICE_H_
#define HTTPSERVICE_H_

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <microhttpd.h>

#define METHOD_GET(method) (!strncmp(method, "GET", strlen("GET")))
#define METHOD_POST(method) (!strncmp(method, "POST", strlen("POST")))

#define IS_HTTP_FUNC(url, func) (!strncmp(url, func, strlen(func)))

typedef struct {
	struct MHD_Daemon * daemon;
	void (*start)(int port);
	void (*stop)();
	int (*dispatch)(void *, struct MHD_Connection *, const char *, const char *,
			const char *, const char *, size_t *, void **);
} HTTPService;

HTTPService * getHTTPService();

#endif /* HTTPSERVICE_H_ */
