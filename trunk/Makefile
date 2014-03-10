CC=gcc
CFLAGS=-g -O0 -I$(IDIR) $(DSIZE)
IDIR=include
LDIR=lib
DEPS=include/revdedup.h include/queue.h include/datatable.h
LIB=-lpthread -lcrypto -lm -lkyotocabinet -lmicrohttpd -lcurl

CHOBJ = chunking.o fingerprint.o rabin.o
CDOBJ = bloom.o bucket.o compress.o image.o index.o convdedup.o minilzo.o
RDOBJ = revdedup.o revmap.o revrbd.o revref.o minilzo.o
RDMOBJ = revdedup_meta.o revmap.o revrbd_meta.o revref.o minilzo.o
RSOBJ = restore.o minilzo.o
ROOBJ = restoreo.o minilzo.o
RMOBJ = restore_meta.o minilzo.o
ROMOBJ = restoreo_meta.o minilzo.o
REOBJ = remove.o
DEOBJ = delete.o
DOOBJ = deleteo.o
SVOBJ = server.o index.o image.o compress.o bucket.o minilzo.o
SMOBJ = server_meta.o index.o image.o compress_meta.o bucket_meta.o minilzo.o
CLOBJ = client.o
CMOBJ = client_meta.o
BSOBJ = count_bucketsize.o 
RDMOBJ = readmeta.o 

all: chunking convdedup revdedup restore restoreo remove delete deleteo server client client_meta server_meta restore_meta restoreo_meta revdedup_meta count_bucketsize readmeta
	
%.o : %.c $(DEPS)
	$(CC)  -c -o $@ $< $(CFLAGS)

readmeata: $(RDMOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o readmeta $^ $(LIB)
	
count_bucketsize: $(BSOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o count_bucketsize $^ $(LIB)
		
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

client_meta: $(CMOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o client_meta $^ $(LIB)

server_meta: $(SMOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o server_meta $^ $(LIB)

restore_meta: $(RMOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o restore_meta $^ $(LIB)
	
restoreo_meta: $(ROMOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o restoreo_meta $^ $(LIB)

revdedup_meta: $(RDMOBJ)
	$(CC) $(CFLAGS) -L$(LDIR) -o revdedup_meta $^ $(LIB)

clean:
	rm -rf chunking convdedup revdedup restore restoreo remove delete deleteo server client client_meta server_meta restore_meta restoreo_meta revdedup_meta *.o
