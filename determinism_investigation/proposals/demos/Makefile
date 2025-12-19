# Makefile for non-determinism investigation demos
#
# Build: make
# Clean: make clean
# Build individual: make clock_comparison

CXX = g++
CXXFLAGS = -std=c++20 -O2 -g -Wall -Wextra -pthread

# Undo tools paths
LIVE_RECORD = ~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/live-record
UDB = ~/Downloads/Undo-Suite-Corporate-Multiarch-9.1.0/udb

# All demos
DEMOS = clock_comparison scheduler_timing clock_separate_test \
        high_res_clock_test consolidated_scheduler timerfd_scheduler \
        timer_backend_comparison

.PHONY: all clean record-% analyze-%

all: $(DEMOS)

clock_comparison: clock_comparison.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

scheduler_timing: scheduler_timing.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clock_separate_test: clock_separate_test.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

high_res_clock_test: high_res_clock_test.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

consolidated_scheduler: consolidated_scheduler.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

timerfd_scheduler: timerfd_scheduler.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

timer_backend_comparison: timer_backend_comparison.cpp
	$(CXX) $(CXXFLAGS) -lrt -o $@ $<

clean:
	rm -f $(DEMOS) *.undo

# Record a demo with live-record
# Usage: make record-clock_comparison
record-%: %
	$(LIVE_RECORD) -o $<.undo ./$<

# Analyze a recording with udb (interactive)
# Usage: make analyze-clock_comparison
analyze-%: %.undo
	$(UDB) $<

# Quick syscall analysis with strace
# Usage: make strace-clock_comparison
strace-%: %
	strace -c ./$<

# Detailed syscall trace
# Usage: make strace-detail-clock_comparison
strace-detail-%: %
	strace -e clock_gettime,gettimeofday,futex ./$<

help:
	@echo "Available targets:"
	@echo "  all                  - Build all demos"
	@echo "  clean                - Remove binaries and recordings"
	@echo "  clock_comparison     - Build clock comparison demo"
	@echo "  scheduler_timing     - Build scheduler timing demo"
	@echo ""
	@echo "Recording and analysis:"
	@echo "  record-<demo>        - Record demo with live-record"
	@echo "  analyze-<demo>       - Analyze recording with udb"
	@echo "  strace-<demo>        - Quick syscall stats with strace"
	@echo "  strace-detail-<demo> - Detailed syscall trace"
	@echo ""
	@echo "Examples:"
	@echo "  make clock_comparison"
	@echo "  make record-clock_comparison"
	@echo "  make strace-clock_comparison"
