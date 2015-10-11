CXX=clang++
CFLAGS=-std=c++1y -O2 -Wall -Wno-unknown-pragmas
LDFLAGS=-luv -lhttp_parser speedy/r3/.libs/libr3.a -L/usr/local/lib -lpcre -pthread

INC=-I./
SOURCES=$(wildcard *.cpp)
EXECUTABLE=memgraph

all: $(EXECUTABLE)
	    
$(EXECUTABLE): $(SOURCES) 
	$(CXX) $(CFLAGS) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS) 

.PHONY:
clean:
	rm -f memgraph
	rm -f *.o
