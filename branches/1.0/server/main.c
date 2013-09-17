#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include "rdserver.h"
#include "rdservice.h"

void sighandler(int signal) {
	return;
}


int main(int argc, const char * argv[]) {
	signal(SIGUSR1, sighandler);
	TDService * tds = getTDService();
	tds->start();
	pause();
	tds->stop();
	return 0;
}
