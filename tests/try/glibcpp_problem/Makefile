# compiler
CXX=clang++

# compile flags
CFLAGS=-std=c++1y -pthread -g2 # -D_GLIBCXX_DEBUG

# includes and libraries
INCLUDE_PATHS=-I../../../include -I../../../libs/fmt -I../../../src
LIB_PATHS=-L../../../libs/fmt/fmt
LDFLAGS=-lfmt

# source and executable
LOG_SRC_PATH=../../..
SOURCES=main.cpp async_log.o sync_log.o stderr.o stdout.o default.o levels.o log.o
EXECUTABLE=a.out

# release target
all: $(EXECUTABLE)

$(EXECUTABLE): $(SOURCES)
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) $(SOURCES) -o $(EXECUTABLE) $(LIB_PATHS) $(LDFLAGS)

# TODO: auto
async_log.o: ../../../src/logging/logs/async_log.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/logs/async_log.cpp

sync_log.o: ../../../src/logging/logs/sync_log.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/logs/sync_log.cpp

stderr.o: ../../../src/logging/streams/stderr.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/streams/stderr.cpp

stdout.o: ../../../src/logging/streams/stdout.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/streams/stdout.cpp

default.o: ../../../src/logging/default.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/default.cpp

levels.o: ../../../src/logging/levels.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/levels.cpp

log.o: ../../../src/logging/log.cpp
	$(CXX) $(CFLAGS) $(INCLUDE_PATHS) -c ../../../src/logging/log.cpp

.PHONY:
clean:
	rm -f a.out
	rm -f *.o
