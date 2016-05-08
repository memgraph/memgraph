CXX=clang++
CFLAGS=-std=c++1y -DNDEBUG -O2 -Wall -Wno-unknown-pragmas
CFLAGS_DEBUG=-std=c++1y -Wall -Wno-unknown-pragmas -g
LDFLAGS=-luv -lhttp_parser src/speedy/r3/.libs/libr3.a -L/usr/local/lib -lpcre -pthread

INC=-I./src/ -I./src/speedy/ -I./src/speedy/rapidjson/include/
SOURCES=src/memgraph.cpp
EXECUTABLE=build/memgraph

all: $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CXX) $(CFLAGS) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS)

debug: $(SOURCES)
	$(CXX) $(CFLAGS_DEBUG) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS)

.PHONY:
clean:
	rm -f memgraph
	rm -f *.o
