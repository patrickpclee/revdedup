/*
 * database.c
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */

#include <stdlib.h>
#include <bson.h>
#include "tdserver.h"
#include "database.h"
#include "image.h"
#include "segment.h"
#include "fingerprint.h"

#define MONGO_CONN_CNT 32

static DBService service;

DBResource * getConn() {
	DBResource * resource;
	pthread_mutex_lock(&service.global_lock);
	pthread_mutex_lock(&service.resource[service.conn_ptr].lock);
	resource = &service.resource[service.conn_ptr++];
	pthread_mutex_unlock(&service.global_lock);
	return resource;
}

void putConn(DBResource * resource) {
	pthread_mutex_unlock(&resource->lock);
}

/*
 * Image Database Hack
 * Using in-memory database
 */
static uint64_t image_cnt = 0;
static HLIST_HEAD(image_head);
static pthread_spinlock_t image_lock;

static Image * findImage(const char * name) {
	Image * image;
	hlist_node * node;
	pthread_spin_lock(&image_lock);
	hlist_for_each_entry(image, node, &image_head, node) {
		if (!strncmp(image->name, name, STRING_BUF_SIZE)) {
			pthread_spin_unlock(&image_lock);
			return image;
		}
	}
	pthread_spin_unlock(&image_lock);
	return NULL;
}

static void insertImage(Image * image) {
	pthread_spin_lock(&image_lock);
	hlist_add_head(&image->node, &image_head);
	image_cnt++;
	pthread_spin_unlock(&image_lock);
}

static void updateImage(Image * image) {

}

static ImageVersion * findImageVersion(Image * image, uint64_t version) {
	ImageVersion * iv;
	hlist_node * node;
	pthread_spin_lock(&image->lock);
	hlist_for_each_entry(iv, node, &image->head, node) {
		if (iv->version == version) {
			pthread_spin_unlock(&image->lock);
			return iv;
		}
	}
	pthread_spin_unlock(&image->lock);
	return NULL;
}

static void insertImageVersion(ImageVersion * version) {

}

static void updateImageVersion(ImageVersion * version) {

}

static void getAllImageVersions();
static void putImageVersions(DBResource * resource, Image * img);


static void getAllImages() {
	mongo_cursor cursor[1];
	mongo_cursor_init(cursor, &service.resource[0].conn, "tdserver.image");

	while (mongo_cursor_next(cursor) == MONGO_OK) {
		bson_iterator it[1];
		Image * img = malloc(sizeof(Image));
		memset(img, 0, sizeof(Image));
		bson_find(it, mongo_cursor_bson(cursor), "nm");
		strcpy(img->name, bson_iterator_string(it));
		bson_find(it, mongo_cursor_bson(cursor), "vr");
		img->versions = bson_iterator_long(it);
		pthread_spin_init(&img->lock, PTHREAD_PROCESS_PRIVATE);
		hlist_add_head(&img->node, &image_head);
		image_cnt++;
	}
	mongo_cursor_destroy(cursor);
}

static void putAllImages() {
	DBResource * resource = getConn();
	hlist_node * node;
	Image * img;
	bson result[1];
	int i;

	mongo_cmd_drop_collection(&resource->conn, "tdserver", "imgver", result);
	mongo_cmd_drop_collection(&resource->conn, "tdserver", "image", result);

	uint64_t img_ptr = 0;
	bson ** img_bsons = malloc(image_cnt * sizeof(bson *));
	hlist_for_each_entry(img, node, &image_head, node)
	{
		bson * img_bson = malloc(sizeof(bson));
		bson_init(img_bson);
		bson_append_string(img_bson, "nm", img->name);
		bson_append_long(img_bson, "vr", img->versions);
		bson_finish(img_bson);
		img_bsons[img_ptr++] = img_bson;
		putImageVersions(resource, img);
	}
	mongo_insert_batch(&resource->conn, "tdserver.image", (const bson **)img_bsons, img_ptr,
			NULL, 0);
	for (i = 0; i < image_cnt; i++) {
		free(img_bsons[i]);
	}
	free(img_bsons);
	putConn(resource);
}

#include <stdio.h>

static void getAllImageVersions() {
	char name[STRING_BUF_SIZE];
	mongo_cursor cursor[1];
	mongo_cursor_init(cursor, &service.resource[0].conn, "tdserver.imgver");

	while (mongo_cursor_next(cursor) == MONGO_OK) {
		bson_iterator it[1];
		ImageVersion * iv = malloc(sizeof(ImageVersion));
		memset(iv, 0, sizeof(ImageVersion));
		bson_find(it, mongo_cursor_bson(cursor), "nm");
		strcpy(name, bson_iterator_string(it));
		bson_find(it, mongo_cursor_bson(cursor), "vr");
		iv->version = bson_iterator_long(it);
		bson_find(it, mongo_cursor_bson(cursor), "sz");
		iv->size = bson_iterator_long(it);
		bson_find(it, mongo_cursor_bson(cursor), "sc");
		iv->seg_cnt = bson_iterator_int(it);
		bson_find(it, mongo_cursor_bson(cursor), "sf");
		iv->segmentfps = malloc(iv->seg_cnt * sizeof(Fingerprint));
		memcpy(iv->segmentfps, bson_iterator_bin_data(it), bson_iterator_bin_len(it));
		Image * img = findImage(name);
		iv->img = img;
		pthread_spin_lock(&img->lock);
		hlist_add_head(&iv->node, &img->head);
		pthread_spin_unlock(&img->lock);
/*
		char buffer1[STRING_BUF_SIZE];
		char buffer2[STRING_BUF_SIZE];
		int i;
		sprintf(buffer1, "fp%d", iv->version);
		FILE * filep = fopen(buffer1, "a");
		for (i = 0; i < iv->seg_cnt; i++) {
			bin2hex(&iv->segmentfps[i], buffer2);
			fprintf(filep, "%s\n", buffer2);
		}
		fclose(filep);
*/
	}
	mongo_cursor_destroy(cursor);
}

static void putImageVersions(DBResource * resource, Image * img) {
	ImageVersion * iv;
	hlist_node * node;

	uint64_t iv_ptr = 0;
	const bson ** iv_bsons = malloc(img->versions * sizeof(bson *));
	hlist_for_each_entry(iv, node, &img->head, node)
	{
		bson * iv_bson = malloc(sizeof(bson));
		bson_init(iv_bson);
		bson_append_string(iv_bson, "nm", img->name);
		bson_append_long(iv_bson, "vr", iv->version);
		bson_append_long(iv_bson, "sz", iv->size);
		bson_append_int(iv_bson, "sc", iv->seg_cnt);
		bson_append_binary(iv_bson, "sf", BSON_BIN_BINARY, (const char *)iv->segmentfps,
				FP_SIZE * iv->seg_cnt);
		bson_finish(iv_bson);
		iv_bsons[iv_ptr++] = iv_bson;
	}
	mongo_insert_batch(&resource->conn, "tdserver.imgver", iv_bsons, iv_ptr,
			NULL, 0);
}
/*
 * Segment Database Hack
 * Because using mongodb seems slow
 * We now use an in-memory hash table
 */
#define SEGMENT_HASH_TABLE_MASK 0x7FFFF
#define SEGMENT_HASH_TABLE_SIZE 524288

static uint64_t segment_cnt = 0;
static HashHead segment_ht[SEGMENT_HASH_TABLE_SIZE];

static Segment * findSegment(Fingerprint * fp) {
	uint64_t p = hash(fp, SEGMENT_HASH_TABLE_MASK);
	hlist_head * head = &segment_ht[p].head;
	hlist_node * node;
	Segment * ptr;
	pthread_spin_lock(&segment_ht[p].lock);
	hlist_for_each_entry(ptr, node, head, node) {
		if (FP_EQUAL(&ptr->fp, fp)) {
			pthread_spin_unlock(&segment_ht[p].lock);
			return ptr;
		}
	}
	pthread_spin_unlock(&segment_ht[p].lock);
	return NULL ;
}

static int insertSegment(Segment * seg) {
	uint64_t p = hash(&seg->fp, SEGMENT_HASH_TABLE_MASK);
	hlist_head * head = &segment_ht[p].head;
	hlist_node * node;
	Segment * ptr;
	pthread_spin_lock(&segment_ht[p].lock);

	hlist_for_each_entry(ptr, node, head, node)
	{
		if (FP_EQUAL(&ptr->fp, &seg->fp)) {
			pthread_spin_unlock(&segment_ht[p].lock);
			return -1;
		}
	}
	hlist_add_head(&seg->node, &segment_ht[p].head);
	segment_cnt++;
	pthread_spin_unlock(&segment_ht[p].lock);
	return 0;
}

static void updateSegment(Segment * seg) {

}

static void getAllSegments() {
	mongo_cursor cursor[1];
	mongo_cursor_init(cursor, &service.resource[0].conn, "tdserver.segment");

	while (mongo_cursor_next(cursor) == MONGO_OK) {
		bson_iterator it[1];
		Segment * seg = malloc(sizeof(Segment));
		memset(seg, 0, sizeof(Segment));
		bson_find(it, mongo_cursor_bson(cursor), "fp");
		memcpy(&seg->fp, bson_iterator_bin_data(it), bson_iterator_bin_len(it));
		bson_find(it, mongo_cursor_bson(cursor), "rc");
		seg->refcnt = bson_iterator_int(it);
		bson_find(it, mongo_cursor_bson(cursor), "sz");
		seg->size = bson_iterator_int(it);
		hlist_add_head(&seg->node,
				&segment_ht[hash(&seg->fp, SEGMENT_HASH_TABLE_MASK)].head);
		segment_cnt++;
	}
	mongo_cursor_destroy(cursor);
}

static void putAllSegments() {
	DBResource * resource = getConn();
	hlist_node * node;
	Segment * seg;
	bson result[1];
	int i;
	uint64_t seg_cnt = 0;
	for (i = 0; i < SEGMENT_HASH_TABLE_SIZE; i++) {
		hlist_for_each(node, &segment_ht[i].head) {
			seg_cnt++;
		}
	}
	printf("Inserting %lu segments\n", seg_cnt);
	mongo_cmd_drop_collection(&resource->conn, "tdserver", "segment", result);
	bson * seg_bson = malloc(sizeof(bson));
	for (i = 0; i < SEGMENT_HASH_TABLE_SIZE; i++) {
		hlist_for_each_entry(seg, node, &segment_ht[i].head, node)
		{
			bson_init(seg_bson);
			bson_append_binary(seg_bson, "fp", BSON_BIN_BINARY,
					(char *) seg->fp.x, FP_SIZE);
			bson_append_int(seg_bson, "rc", seg->refcnt);
			bson_append_int(seg_bson, "sz", seg->size);
			bson_finish(seg_bson);
			mongo_insert(&resource->conn, "tdserver.segment", seg_bson, NULL);
			bson_destroy(seg_bson);
		}
	}
	free(seg_bson);
	putConn(resource);
}

static int start() {
	int i, status;
	service.resource = malloc(MONGO_CONN_CNT * sizeof(DBResource));
	service.conn_ptr = 0;
	pthread_mutex_init(&service.global_lock, NULL);
	mongo_write_concern_init(&service.wc);
	service.wc.w = 1;
	mongo_write_concern_finish(&service.wc);
	for (i = 0; i < MONGO_CONN_CNT; i++) {
		mongo_init(&service.resource[i].conn);
		status = mongo_client(&service.resource[i].conn, "127.0.0.1", 27017);
		if (status != MONGO_OK) {
			switch (service.resource[i].conn.err) {
			case MONGO_CONN_NO_SOCKET:
				printf("no socket\n");
				return -1;
			case MONGO_CONN_FAIL:
				printf("connection failed\n");
				return -1;
			case MONGO_CONN_NOT_MASTER:
				printf("not master\n");
				return -1;
			default:
				break;
			}
		}
		pthread_mutex_init(&service.resource[i].lock, NULL);
		mongo_set_write_concern(&service.resource[i].conn, &service.wc);
	}

	pthread_spin_init(&image_lock, PTHREAD_PROCESS_PRIVATE);

	for (i = 0; i < SEGMENT_HASH_TABLE_SIZE; i++) {
		INIT_HLIST_HEAD(&segment_ht[i].head);
		pthread_spin_init(&segment_ht[i].lock, PTHREAD_PROCESS_PRIVATE);
	}
	getAllSegments();
	getAllImages();
	getAllImageVersions();
	return 0;
}

static void stop() {
	int i;
	putAllImages();
	putAllSegments();
	for (i = 0; i < MONGO_CONN_CNT; i++) {
		mongo_destroy(&service.resource[i].conn);
	}
}

static DBService service = {
		.start = start,
		.stop = stop,
		.findSegment = findSegment,
		.insertSegment = insertSegment,
		.updateSegment = updateSegment,

		.findImage = findImage,
		.insertImage = insertImage,
		.updateImage = updateImage,

		.findImageVersion = findImageVersion,
		.insertImageVersion = insertImageVersion,
		.updateImageVersion = updateImageVersion,
};

DBService * getDBService() {
	return &service;
}
