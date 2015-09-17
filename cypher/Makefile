CXX = clang++
CXXFLAGS = -std=c++11
INC = -I../

parser: parser.o cypher.o
	$(CXX) parser.o cypher.o -o parser $(INC) $(CXXFLAGS)

cypher.o:
	$(CXX) cypher.cpp -c -o cypher.o $(INC) $(CXXFLAGS)

parser.o: parser.cpp cypher.hpp
	$(CXX) parser.cpp -c -o parser.o $(INC) $(CXXFLAGS)

cypher.cpp: lemonfiles

cypher.hpp: lemonfiles

.PHONY: lemonfiles
lemonfiles: cypher.y
	lemon cypher.y -s
	mv cypher.c cypher.cpp

.PHONY: clean
clean:
	rm -f *.o
	rm -f cypher.cpp
	rm -f cypher.h
	rm -f parser
	rm -f cypher.out
