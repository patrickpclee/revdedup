/**
 * @file	bucket.h
 * @brief	Bucket Service Definition
 * @author	Ng Chun Ho
 */

#ifndef BUCKET_H_
#define BUCKET_H_

#include <revdedup.h>
#include <queue.h>

/**
 * Bucket service control
 */
typedef struct {
	pthread_t _tid;		/*!< Thread ID for processing segments */
	Queue * _iq;		/*!< Queue for incoming segments */
	Queue * _oq;		/*!< Queue for outgoing segments */
	BMEntry * _en;		/*!< Bucket entries */
	BucketLog * _log;	/*!< Pointer to global bucket log */
	uint8_t _padding[BLOCK_SIZE];	/*!< Padding for RAID-5 or 6 systems */
	/**
	 * Starts the bucket service
	 * @param iq		Queue for incoming segments
	 * @param oq		Queue for outgoing segments
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(Queue * iq, Queue * oq);
	/**
	 * Stops the bucket service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} BucketService;

/**
 * Gets the control of bucket service
 * @return				The bucket service
 */
BucketService* GetBucketService();

#endif /* BUCKET_H_ */
