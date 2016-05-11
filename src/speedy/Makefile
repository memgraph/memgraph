CXX=clang++
CFLAGS=-std=c++11 -Wall -O2
LDFLAGS=-luv -lhttp_parser ./r3/.libs/libr3.a -L/usr/local/lib -lpcre
# debug only
INC=-I../
SOURCES=$(wildcard *.cpp)
EXECUTABLE=test.out
# OBJECTS=$(SOURCES:.cpp=.o)

all: $(EXECUTABLE)
	    
$(EXECUTABLE): $(SOURCES) 
	$(CXX) $(CFLAGS) $(SOURCES) -o $(EXECUTABLE) $(INC) $(LDFLAGS) 

# .cpp.o:
#	$(CXX) $(CFLAGS) $< -o $@ $(LDFLAGS) $(INC)

.PHONY:
clean:
	rm -f *.out
	rm -f *.o
