/**
 * @file	index.h
 * @brief	Index Service Definition
 * @author	Ng Chun Ho
 */

#ifndef INDEX_H_
#define INDEX_H_

#include <revdedup.h>
#include <queue.h>
#include <kclangc.h>
#include "bloom.h"

/**
 * Index service control
 */
typedef struct {
	pthread_t _tid;			/*!< Thread ID for processing segments */
	Queue * _iq;			/*!< Queue for incoming segments */
	Queue * _oq;			/*!< Queue for outgoing segments */
	SMEntry * _sen;			/*!< Segment entries */
	CMEntry * _cen;			/*!< Chunk entries */
	SegmentLog * _slog;		/*!< Pointer to global segment log */
	ChunkLog * _clog;		/*!< Pointer to global chunk log */
	KCDB * _db;				/*!< Fingerprint -> Segment ID database */
	Bloom _bl;				/*!< Bloom filter for segment fingerprint */
	/**
	 * Starts the index service
	 * @param iq		Queue for incoming segments
	 * @param oq		Queue for outgoing segments
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(Queue * iq, Queue * oq);
	/**
	 * Stops the index service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
	/**
	 * Write segment metadata in memory to the disk
	 * @param seg		Segment to write
	 * @param bucket	Bucket ID containing the segment
	 * @return			0 if successful, -1 otherwise
	 */
	int (*putSegment)(Segment * seg, uint64_t bucket);
	/**
	 * Read segment metadata to memory from the disk
	 * @param seg		Segment to read
	 * @return			0 if successful, -1 otherwise
	 */
	int (*getSegment)(Segment * seg);
} IndexService;

/**
 * Gets the control of index service
 * @return				The index service
 */
IndexService* GetIndexService();

#endif /* INDEX_H_ */
