/**
 * @file	revmap.h
 * @brief	Reverse Deduplication Mapping Service Definition
 * @author	Ng Chun Ho
 */

#ifndef REVMAP_H_
#define REVMAP_H_

#include <revdedup.h>

/**
 *	Reverse deduplication mapping service control
 */
typedef struct {
	pthread_t _tid[REV_CNT];		/*!< Thread ID for processing images */
	SMEntry * _sen;					/*!< Segment entries */
	CMEntry * _cen;					/*!< Chunk entries */
	SegmentLog * _slog;				/*!< Pointer to global segment log */
	ChunkLog * _clog;				/*!< Pointer to global chunk log */
	uint32_t _ins;					/*!< Total images in revdedup */
	uint32_t _ver;					/*!< Version for reverse deduplication */
	pthread_spinlock_t _rlock;		/*!< Lock to synchronize chunk reference count */
	/**
	 * Starts the mapping service
	 * @param sen		Segment entries
	 * @param cen		Chunk entries
	 * @param images	Total images in revdedup
	 * @param version	Version for reverse deduplication
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(SMEntry * sen, CMEntry * cen, uint32_t images, uint32_t version);
	/**
	 *	Stops the mapping service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} RevMapService;

/**
 * Gets the control of mapping service
 * @return				The reference count service
 */
RevMapService * GetRevMapService();

#endif /* REVMAP_H_ */
