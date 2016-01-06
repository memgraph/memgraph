CXX=clang++
CFLAGS=-std=c++1y -O2 -Wall -Wno-unknown-pragmas
CFLAGS_DEBUG=-std=c++1y -Wall -Wno-unknown-pragmas -g
LDFLAGS=-luv -lhttp_parser speedy/r3/.libs/libr3.a -L/usr/local/lib -lpcre -pthread

INC=-I./ -I./speedy/rapidjson/include/
SOURCES=memgraph.cpp
EXECUTABLE=memgraph

all: $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CXX) $(CFLAGS) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS)

debug: $(SOURCES)
	$(CXX) $(CFLAGS_DEBUG) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS)

.PHONY:
clean:
	rm -f memgraph
	rm -f *.o
