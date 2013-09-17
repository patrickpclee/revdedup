/*
 * fpcompute.c
 *
 *  Created on: Dec 7, 2012
 *      Author: chng
 */


#include "fpcompute.h"
#include <openssl/sha.h>

static unsigned short bin2hex_t[] = { 12336, 12592, 12848, 13104, 13360, 13616, 13872,
		14128, 14384, 14640, 24880, 25136, 25392, 25648, 25904, 26160, 12337,
		12593, 12849, 13105, 13361, 13617, 13873, 14129, 14385, 14641, 24881,
		25137, 25393, 25649, 25905, 26161, 12338, 12594, 12850, 13106, 13362,
		13618, 13874, 14130, 14386, 14642, 24882, 25138, 25394, 25650, 25906,
		26162, 12339, 12595, 12851, 13107, 13363, 13619, 13875, 14131, 14387,
		14643, 24883, 25139, 25395, 25651, 25907, 26163, 12340, 12596, 12852,
		13108, 13364, 13620, 13876, 14132, 14388, 14644, 24884, 25140, 25396,
		25652, 25908, 26164, 12341, 12597, 12853, 13109, 13365, 13621, 13877,
		14133, 14389, 14645, 24885, 25141, 25397, 25653, 25909, 26165, 12342,
		12598, 12854, 13110, 13366, 13622, 13878, 14134, 14390, 14646, 24886,
		25142, 25398, 25654, 25910, 26166, 12343, 12599, 12855, 13111, 13367,
		13623, 13879, 14135, 14391, 14647, 24887, 25143, 25399, 25655, 25911,
		26167, 12344, 12600, 12856, 13112, 13368, 13624, 13880, 14136, 14392,
		14648, 24888, 25144, 25400, 25656, 25912, 26168, 12345, 12601, 12857,
		13113, 13369, 13625, 13881, 14137, 14393, 14649, 24889, 25145, 25401,
		25657, 25913, 26169, 12385, 12641, 12897, 13153, 13409, 13665, 13921,
		14177, 14433, 14689, 24929, 25185, 25441, 25697, 25953, 26209, 12386,
		12642, 12898, 13154, 13410, 13666, 13922, 14178, 14434, 14690, 24930,
		25186, 25442, 25698, 25954, 26210, 12387, 12643, 12899, 13155, 13411,
		13667, 13923, 14179, 14435, 14691, 24931, 25187, 25443, 25699, 25955,
		26211, 12388, 12644, 12900, 13156, 13412, 13668, 13924, 14180, 14436,
		14692, 24932, 25188, 25444, 25700, 25956, 26212, 12389, 12645, 12901,
		13157, 13413, 13669, 13925, 14181, 14437, 14693, 24933, 25189, 25445,
		25701, 25957, 26213, 12390, 12646, 12902, 13158, 13414, 13670, 13926,
		14182, 14438, 14694, 24934, 25190, 25446, 25702, 25958, 26214
};


#define FP_ZERO(x) (!memcmp((x), "\x1c\xea\xf7\x3d\xf4\x0e\x53\x1d\xf3\xbf\xb2\x6b\x4f\xb7\xcd\x95\xfb\x7b\xff\x1d", FP_SIZE))

void printhex(unsigned char * fp) {
	int i;
	for (i = 0; i < FP_SIZE; i++) {
		printf("%02x", fp[i]);
	}
	printf("\n");
}


void bin2hex(unsigned char * fp, char * out) {
	int i;
	unsigned short * tmp = (unsigned short *)out;
	for (i = 0; i < FP_SIZE; i++) {
		tmp[i] = bin2hex_t[fp[i]];
	}
	tmp[FP_SIZE] = 0;
}

static FPCompService service;

static void * run(void * ptr) {
	Segment * seg;
	int i;
	unsigned char md[FP_SIZE];
	while ((seg = queue_pop(service.in)) != NULL) {
		memset(&seg->blockfp, 0, FP_SIZE * SEG_BLOCKS);
		for (i = 0; i < seg->blocks; i++) {
			SHA1(seg->data + i * BLOCK_SIZE, BLOCK_SIZE, seg->blockfp[i]);
		}
		SHA1((uint8_t *)seg->blockfp, seg->blocks * FP_SIZE, md);
		bin2hex(md, seg->segmentfp);
	}
	return NULL;
}

static void start(Queue * in, Queue * out) {
	int i;
	service.threads = malloc(FPCOMP_THREAD_CNT * sizeof(pthread_t));
	service.in = in;
	service.out = out;

	for (i = 0; i < FPCOMP_THREAD_CNT; i++) {
		pthread_create(&service.threads[i], NULL, run, NULL);
	}
}

static void stop() {
	int i;
	for (i = 0; i < FPCOMP_THREAD_CNT; i++) {
		queue_push(NULL, service.in);
	}
	for (i = 0; i < FPCOMP_THREAD_CNT; i++) {
		pthread_join(service.threads[i], NULL);
	}
}

static FPCompService service = {
		.start = start,
		.stop = stop,
};

FPCompService * getFPCompService () {
	return &service;
}

