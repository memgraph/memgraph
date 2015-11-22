CXX=clang++
CFLAGS=-std=c++1y -O2 -Wall -Wno-unknown-pragmas
LDFLAGS=-lhttp_parser -pthread

INC=-I../../
SOURCES=test.cpp
EXECUTABLE=test

all: $(EXECUTABLE)
	    
$(EXECUTABLE): $(SOURCES) 
	$(CXX) $(CFLAGS) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS) 

.PHONY:
clean:
	rm -f test
	rm -f *.o
