/**
 * @file	compress.h
 * @brief	Compression Service Definition
 * @author	Ng Chun Ho
 */

#ifndef COMPRESS_H_
#define COMPRESS_H_

#include <revdedup.h>
#include <queue.h>

/**
 * Compression service control
 */
typedef struct {
	pthread_t _tid;				/*!< Thread ID for distributing segments */
	pthread_t _gid;				/*!< Thread ID for gathering segments */
	pthread_t _cid[CPS_CNT];	/*!< Thread IDs for processing segments */
	Queue * _iq;				/*!< Queue for incoming segments */
	Queue * _oq;				/*!< Queue for outgoing segments */
	Queue * _mq[CPS_CNT];		/*!< Distributed queue containing incoming segments for each thread */
	Queue * _dq[CPS_CNT];		/*!< Distributed queue containing outgoing segments for each thread */
	/**
	 * Starts the compression service
	 * @param iq		Queue for incoming segments
	 * @param oq		Queue for outgoing segments
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(Queue * iq, Queue * oq);
	/**
	 * Stops the compression service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} CompressService;

/**
 * Gets the control of compression service
 * @return				The compression service
 */
CompressService* GetCompressService();

#endif /* COMPRESS_H_ */
