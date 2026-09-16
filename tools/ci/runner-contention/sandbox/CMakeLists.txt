cmake_minimum_required(VERSION 3.20)
project(ctest_sched_probe NONE)
include(CTest)
enable_testing()

# Each test records its start and end so actual concurrency can be reconstructed
# from the log, rather than inferred from total wall time.
set(RUNNER ${CMAKE_CURRENT_SOURCE_DIR}/slot.sh)

# Group A: one disk slot each. With a spec declaring N slots, at most N of these
# should overlap regardless of -j.
foreach(i RANGE 1 8)
  add_test(NAME disk_${i} COMMAND ${RUNNER} disk_${i} 2)
  set_tests_properties(disk_${i} PROPERTIES RESOURCE_GROUPS "disk:1")
endforeach()

# Group B: PROCESSORS weighting. With -j8 and PROCESSORS 4, at most 2 overlap.
foreach(i RANGE 1 4)
  add_test(NAME proc4_${i} COMMAND ${RUNNER} proc4_${i} 2)
  set_tests_properties(proc4_${i} PROPERTIES PROCESSORS 4)
endforeach()

# Group C: PROCESSORS larger than any plausible -j. Does it still run?
add_test(NAME proc_huge COMMAND ${RUNNER} proc_huge 1)
set_tests_properties(proc_huge PROPERTIES PROCESSORS 64)

# Group D: demands more slots of a resource than the spec will declare.
# Expected to report Not Run rather than fail, which is the silent-skip hazard.
add_test(NAME disk_greedy COMMAND ${RUNNER} disk_greedy 1)
set_tests_properties(disk_greedy PROPERTIES RESOURCE_GROUPS "disk:99")
