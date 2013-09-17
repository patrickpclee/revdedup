CC=clang
CFLAGS=-g -O2 -I$(IDIR)
IDIR=include
LDIR=lib
DEPS=include/revdedup.h include/queue.h include/datatable.h
LIB=-lpthread -lcrypto -lm -lkyotocabinet

CHOBJ = chunking.o fingerprint.o rabin.o
CDOBJ = bloom.o bucket.o compress.o image.o index.o convdedup.o minilzo.o
RDOBJ = btree.o hashtable.o revdedup.o revmap.o revrbd.o revref.o minilzo.o
RSOBJ = restore.o minilzo.o
ROOBJ = restoreo.o minilzo.o
DEOBJ = delete.o

all: chunking convdedup revdedup restore restoreo delete

btree.o : btree.c btree.h
	$(CC)  -c -o $@ $< $(CFLAGS)
	
%.o : %.c $(DEPS)
	$(CC)  -c -o $@ $< $(CFLAGS)
		
chunking: $(CHOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o chunking $^ $(LIB)
	
convdedup: $(CDOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o convdedup $^ $(LIB)
	
revdedup: $(RDOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o revdedup $^ $(LIB)
	
restore: $(RSOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o restore $^ $(LIB)
	
restoreo: $(ROOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o restoreo $^ $(LIB)
	
delete: $(DEOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o delete $^ $(LIB)

clean:
	rm -rf chunking convdedup revdedup restore restoreo delete *.o
