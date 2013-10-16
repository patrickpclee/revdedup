/**
 * @file	fingerprint.h
 * @brief	Fingerprint Service Definition
 * @author	Ng Chun Ho
 */

#ifndef FINGERPRINT_H_
#define FINGERPRINT_H_

#include <revdedup.h>
#include <queue.h>
#include "rabin.h"

/**
 * Fingerprint service control
 */
typedef struct {
	pthread_t _tid;			/*!< Thread ID for processing segments */
	Queue * _iq;			/*!< Queue for incoming segments */
	Queue * _oq;			/*!< Queue for outgoing segments */
	/**
	 * Starts the fingerprint service
	 * @param iq		Queue for incoming segments
	 * @param oq		Queue for outgoing segments
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(Queue * iq, Queue * oq);
	/**
	 * Stops the fingerprint service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} FpService;

/**
 * Gets the control of fingerprint service
 * @return				The fingerprint service
 */
FpService* GetFpService();

#endif /* FINGERPRINT_H_ */
