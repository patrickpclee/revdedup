CC=gcc
CFLAGS=-Wall -O2 -g
LDIR=lib
LDFLAGS=-lpthread -lbson -ljansson -lmicrohttpd -lmongoc -lrt -lgnutls
INC=include

OBJS = httpservice.o fingerprint.o database.o tdservice.o segment.o image.o storage.o main.o 
TARGET = tdserver

%.o: %.c
	$(CC) $(CFLAGS) -I$(INC) -c $< -o $@

all: $(OBJS)
	$(CC) $(CFLAGS)  $(OBJS) -o $(TARGET) -L$(LDIR) $(LDFLAGS)
	
clean:
	rm -rf *.o tdserver
