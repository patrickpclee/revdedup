/*
 * forwardlist.h
 *
 *  Created on: Dec 5, 2012
 *      Author: chng
 */

#ifndef FORWARDLIST_H_
#define FORWARDLIST_H_

/*
 * flhead
 *
 * This implements a single linked-list. This is suitable
 * for systems without deletion (although it is supported)
 * or the length is small. The main advantage is that the
 * space overhead is much smaller (1/2 of double linked-
 * list).
 */

#include <stdlib.h>

typedef struct forward_list_head {
	struct forward_list_head * next;
} flhead;

typedef struct {
	flhead first;
	flhead * last;
} flist;

static inline void flist_init(flist * list) {
	list->first.next = &list->first;
	list->last = &list->first;
}

static inline void flist_add_tail(flhead * head, flist * list) {
	list->last->next = head;
	list->last = head;
	head->next = &list->first;
}

static inline void flist_add(flhead * head, flist * list) {
	if (!list->first.next) {
		flist_add_tail(head, list);
		return;
	}
	head->next = list->first.next;
	list->first.next = head;
}



static inline void flist_del(flhead * head, flist * list) {
	flhead * prev = &list->first;
	while (prev != list->last) {
		if (prev->next == head) {
			prev->next = head->next;
			if (head == list->last) {
				list->last = prev;
			}
			break;
		}
		prev = prev->next;
	}
}


static inline int flist_empty(flist * list) {
	return list->first.next == &list->first;
}


#define flist_entry(ptr, type, member) ({ \
		 const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
		        (type *)( (char *)__mptr - offsetof(type,member) );})

#define flist_first_entry(ptr, type, member) \
		flist_entry((ptr)->first.next, type, member)

#define flist_for_each(pos, head) \
  for (pos = (head)->next; pos != (head);	\
       pos = pos->next)

#define flist_for_each_entry(pos, head, member)				\
	for (pos = list_entry((head)->next, typeof(*pos), member);	\
	     &pos->member != (head);					\
	     pos = flist_entry(pos->member.next, typeof(*pos), member))

#endif /* FORWARDLIST_H_ */
