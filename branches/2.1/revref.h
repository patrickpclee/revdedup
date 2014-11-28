/**
 * @file	revref.h
 * @brief	Reverse Deduplication Reference Service Definition
 * @author	Ng Chun Ho
 */

#ifndef REVREF_H_
#define REVREF_H_

#include <revdedup.h>

/**
 *	Reverse deduplication reference service control
 */
typedef struct {
	pthread_t _tid;			/*!< Thread ID for processing segments */
	SMEntry * _sen;			/*!< Segment entries */
	SegmentLog * _slog;		/*!< Pointer to global segment log */
	uint32_t _ins;			/*!< Total images in revdedup */
	uint32_t _ver;			/*!< Version for reverse deduplication */
	/**
	 * Starts the reference count service
	 * @param sen			Segment entries
	 * @param images		Total images in revdedup
	 * @param version		Version for reverse deduplication
	 * @return				0 if successful, -1 otherwise
	 */
	int (*start)(SMEntry * sen, uint32_t images, uint32_t version);
	/**
	 * Stops the reference count service
	 * @return				0 if successful, -1 otherwise
	 */
	int (*stop)();
} RevRefService;

/**
 * Gets the control of reference count service
 * @return				The reference count service
 */
RevRefService * GetRevRefService();

#endif /* REVSEGREF_H_ */
