CXX=clang++
CFLAGS=-std=c++11 -Wall
LDFLAGS=-luv -lhttp_parser
INC=-I./
SOURCES=$(wildcard *.cpp)
EXECUTABLE=test.out
# OBJECTS=$(SOURCES:.cpp=.o)

all: $(EXECUTABLE)
	    
$(EXECUTABLE): $(SOURCES) 
	$(CXX) $(LDFLAGS) $(SOURCES) -o $(EXECUTABLE) $(CFLAGS)

# .cpp.o:
#	$(CXX) $(CFLAGS) $< -o $@ $(LDFLAGS) $(INC)

.PHONY:
clean:
	rm -f *.out
	rm -f *.o
