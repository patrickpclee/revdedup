/*
 * image.c
 *
 *  Created on: Dec 6, 2012
 *      Author: chng
 */


#include <string.h>
#include "image.h"

Image * newImage(const char * name) {
	Image * image = malloc(sizeof(Image));
	strncpy(image->name, name, STRING_BUF_SIZE - 1);
	image->name[STRING_BUF_SIZE - 1] = '\0';
	image->versions = 0;
	INIT_HLIST_HEAD(&image->head);
	pthread_spin_init(&image->lock, PTHREAD_PROCESS_PRIVATE);
	return image;
}

ImageVersion * newImageVersion(Image * image, uint64_t size, uint32_t seg_cnt,
		Fingerprint * segmentfps) {
	ImageVersion * iv = malloc(sizeof(ImageVersion));
	iv->size = size;
	iv->seg_cnt = seg_cnt;
	iv->img = image;
	iv->segmentfps = malloc(seg_cnt * sizeof(Fingerprint));
	memcpy(iv->segmentfps, segmentfps, seg_cnt * sizeof(Fingerprint));
	pthread_spin_lock(&image->lock);
	iv->version = image->versions++;
	hlist_add_head(&iv->node, &image->head);
	pthread_spin_unlock(&image->lock);
	return iv;
}
