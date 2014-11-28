/**
 * @file	image.h
 * @brief	Image Service Definition
 * @author	Ng Chun Ho
 */

#ifndef IMAGE_H_
#define IMAGE_H_

#include <revdedup.h>
#include <queue.h>

/**
 * Image service control
 */
typedef struct {
	pthread_t _tid;			/*!< Thread ID for processing segments */
	Queue * _iq;			/*!< Queue for incoming segments */
	Queue * _oq;			/*!< Queue for outgoing segments */
	IMEntry * _en;			/*!< Image entries */
	uint32_t _ins;			/*!< Image ID */
	uint32_t _ver;			/*!< Image version */
	int _fd;				/*!< File descriptor of image recipe */
	/**
	 * Starts the image service
	 * @param iq		Queue for incoming segments
	 * @param oq		Queue for outgoing segments
	 * @param imageID	Image ID
	 * @return			0 if successful, -1 otherwise
	 */
	int (*start)(Queue * iq, Queue * oq, uint32_t imageID);
	/**
	 * Stops the image service
	 * @return			0 if successful, -1 otherwise
	 */
	int (*stop)();
} ImageService;

ImageService* GetImageService();

#endif /* IMAGE_H_ */
