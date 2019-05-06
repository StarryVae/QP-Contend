.PHONY: clean

CFLAGS  := -std=c++0x
LD      := g++
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread ./get_clock.h ./get_clock.c

APPS    := server client

all: ${APPS}

client.o: client.cc
	${LD} ${CFLAGS} -c client.cc

client: client.o
	${LD} ${CFLAGS} -o $@ $^ ${LDLIBS}

clean:
	rm -f ${APPS}
