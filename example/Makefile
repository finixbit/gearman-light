CC=g++
CFLAGS=-I.

all: client worker

client: client.cpp
	g++ -o client client.cpp -std=gnu++11 -lgearman 

worker: worker.cpp
	g++ -o worker worker.cpp -std=gnu++11 -lgearman 

clean:
	rm -f worker client 