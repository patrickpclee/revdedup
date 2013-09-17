CC = gcc
CFLAGS = -Wall -g -O0
LIBS := -lcurl -lssl -lpthread -ljansson
LIBPATH := lib
INCPATH := include

all:
	$(CC) $(CFLAGS) -I$(INCPATH) -L$(LIBPATH) -o tdclient *.c $(LIBS)
	
clean:
	rm -f tdclient
