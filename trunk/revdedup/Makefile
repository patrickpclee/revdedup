CC=gcc
CFLAGS=-g -O2 -I$(IDIR) $(DSIZE)
IDIR=include
LDIR=lib
DEPS=include/revdedup.h include/queue.h include/datatable.h
LIB=-lpthread -lcrypto -lm -lkyotocabinet -lmicrohttpd -lcurl

CHOBJ = chunking.o fingerprint.o rabin.o
CDOBJ = bloom.o bucket.o compress.o image.o index.o convdedup.o minilzo.o
RDOBJ = revdedup.o revmap.o revrbd.o revref.o minilzo.o
RSOBJ = restore.o minilzo.o
ROOBJ = restoreo.o minilzo.o
REOBJ = remove.o
DEOBJ = delete.o
DOOBJ = deleteo.o
SVOBJ = server.o index.o image.o compress.o bucket.o minilzo.o
CLOBJ = client.o

all: chunking convdedup revdedup restore restoreo remove delete deleteo server client
	
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
	
remove: $(REOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o remove $^ $(LIB)
	
delete: $(DEOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o delete $^ $(LIB)
	
deleteo: $(DOOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o deleteo $^ $(LIB)

server: $(SVOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o server $^ $(LIB)
	
client: $(CLOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o client $^ $(LIB)

clean:
	rm -rf chunking convdedup revdedup restore restoreo remove delete deleteo server client *.o
