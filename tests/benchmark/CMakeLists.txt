find_package(Threads REQUIRED)

file(GLOB_RECURSE ALL_BENCH_CPP *.cpp)

foreach(ONE_BENCH_CPP ${ALL_BENCH_CPP})

    get_filename_component(ONE_BENCH_EXEC ${ONE_BENCH_CPP} NAME_WE)

    # Avoid name collision 
    set(TARGET_NAME Bench_${ONE_BENCH_EXEC})

    add_executable(${TARGET_NAME} ${ONE_BENCH_CPP})
    set_target_properties(${TARGET_NAME} PROPERTIES OUTPUT_NAME ${ONE_BENCH_EXEC}) 
    target_link_libraries(${TARGET_NAME} benchmark ${CMAKE_THREAD_LIBS_INIT})
    add_test(${TARGET_NAME} ${ONE_BENCH_EXEC})

endforeach()
