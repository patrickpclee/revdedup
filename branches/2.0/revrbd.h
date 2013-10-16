/**
 * @file	revrbd.h
 * @brief	Reverse Deduplication Reconstruction Service Definition
 * @author	Ng Chun Ho
 */


#ifndef REVRBD_H_
#define REVRBD_H_

#include <revdedup.h>
#include <queue.h>
#include <kclangc.h>

/**
 *	Reverse deduplication reconstruction service control
 */
typedef struct {
	pthread_t _tid;				/*!< Thread ID for filtering untouched buckets */
	pthread_t _pbid;			/*!< Thread ID for packing buckets */
	pthread_t _rsid[REV_CNT];	/*!< Thread IDs for segment reconstruction */
	pthread_t _ssid;			/*!< Thread ID for storing reconstructed segments */
	pthread_t _pfid;			/*!< Thread ID for prefetching buckets */
	pthread_t _cbid;			/*!< Thread ID for combining buckets */

	uint32_t _ver;				/*!< Tags the bucket with version for subsequent delete */

	Queue * _pbq;				/*!< Incoming Queue for packing buckets */
	Queue * _rsq;				/*!< Incoming Queue for reconstructing segments */
	Queue * _ssq;				/*!< Incoming Queue for storing reconstructed segments */
	Queue * _pfq;				/*!< Incoming Queue for prefetching buckets */
	Queue * _cbq;				/*!< Thread ID for combining buckets */

	SMEntry * _sen;				/*!< Segment entries */
	SegmentLog * _slog;			/*!< Pointer to global segment log */
	CMEntry * _cen;				/*!< Chunk entries */
	ChunkLog * _clog;			/*!< Pointer to global chunk log */
	BMEntry * _ben;				/*!< Bucket entries */
	BucketLog * _blog;			/*!< Pointer to global bucket log */
	KCDB * _db;					/*!< Fingerprint -> Segment ID database */
	uint8_t _padding[BLOCK_SIZE];		/*!< Padding for RAID-5 or 6 systems */
	/**
	 * Starts the reconstruction service
	 * @param sen		Segment entries
	 * @param cen		Chunk entries
	 * @param ben		Bucket entries
	 * @param ver		Tags the bucket with version for subsequent delete
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(SMEntry * sen, CMEntry * cen, BMEntry * ben, uint32_t ver);
	/**
	 * Stops the reconstruction service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} RevRbdService;

/**
 * Gets the control of reconstruction service
 * @return				The reference count service
 */
RevRbdService * GetRevRbdService();

#endif /* REVRBD_H_ */
