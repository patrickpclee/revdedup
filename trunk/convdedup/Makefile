CC=clang
CFLAGS=-g -O2 -I$(IDIR)
IDIR=include
LDIR=lib
DEPS=include/convdedup.h include/queue.h include/datatable.h
LIB=-lpthread -lcrypto -lm -lkyotocabinet

CHOBJ = chunking.o fingerprint.o rabin.o
CDOBJ = bloom.o btree.o bucket.o compress.o image.o index.o minilzo.o convdedup.o
RSOBJ = restore.o minilzo.o
REOBJ = remove.o
DEOBJ = delete.o

all: chunking convdedup restore remove delete

btree.o : btree.c btree.h
	$(CC)  -c -o $@ $< $(CFLAGS)
	
%.o : %.c $(DEPS)
	$(CC)  -c -o $@ $< $(CFLAGS)
	
chunking: $(CHOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o chunking $^ $(LIB)
	
convdedup: $(CDOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o convdedup $^ $(LIB)
	
restore: $(RSOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o restore $^ $(LIB)
	
remove: $(REOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o remove $^ $(LIB)
	
delete: $(DEOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o delete $^ $(LIB)

clean:
	rm -rf chunking convdedup restore remove delete *.o
