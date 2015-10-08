CXX=clang++
CFLAGS=-std=c++1y -Wall -O2
LDFLAGS=-luv -lhttp_parser speedy/r3/.libs/libr3.a -L/usr/local/lib -lpcre

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
