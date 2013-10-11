/**
 * @file	rabin.h
 * @brief	Rabin Chunking Service Definition
 * @author	Ng Chun Ho
 */

#ifndef RABIN_H_
#define RABIN_H_

#include <revdedup.h>
#include <queue.h>

/**
 * Rabin chunking service control
 */
typedef struct {
	pthread_t _tid;				/*!< Thread ID for creating segments */
	Queue * _q;					/*!< Queue for outgoing segments */
	uint8_t * _data;			/*!< Pointer to data to be chunked */
	int64_t _size;				/*!< Size to data to be chunked */
	/**
	 * Starts the Rabin chunking service
	 * @param data			Pointer to data to be chunked
	 * @param size			Size to data to be chunked
	 * @param q				Queue for outgoing segments
	 * @return				0 if successful, -1 otherwise
	 */
	int (*start)(uint8_t * data, uint64_t size, Queue * q);
	/**
	 * Stops the Rabin chunking service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} RabinService;

/**
 * Gets the control of Rabin Chunking service
 * @return				The Rabin Chunking service
 */
RabinService* GetRabinService();

#endif /* RABIN_H_ */
