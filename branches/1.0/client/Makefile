CC = gcc
CFLAGS = -Wall -g -O2
LIBS := -lcurl -lssl -lpthread -ljansson
LIBPATH := lib
INCPATH := include

all:
	$(CC) $(CFLAGS) -I$(INCPATH) -L$(LIBPATH) -o rdclient *.c $(LIBS)
	
clean:
	rm -f rdclient
