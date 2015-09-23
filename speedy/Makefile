CXX=clang++
CFLAGS=-std=c++11 -Wall
LDFLAGS=-luv -lhttp_parser -lr3
# debug only
INC=-I../ -g -O0 -fno-inline-functions
SOURCES=$(wildcard *.cpp)
EXECUTABLE=test.out
# OBJECTS=$(SOURCES:.cpp=.o)

all: $(EXECUTABLE)
	    
$(EXECUTABLE): $(SOURCES) 
	$(CXX) $(LDFLAGS) $(SOURCES) -o $(EXECUTABLE) $(CFLAGS) $(INC)

# .cpp.o:
#	$(CXX) $(CFLAGS) $< -o $@ $(LDFLAGS) $(INC)

.PHONY:
clean:
	rm -f *.out
	rm -f *.o
