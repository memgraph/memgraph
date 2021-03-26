set(JEMALLOC_DIR "${LIB_DIR}/jemalloc")

set(JEMALLOC_SRCS
    ${JEMALLOC_DIR}/src/arena.c
    ${JEMALLOC_DIR}/src/background_thread.c
    ${JEMALLOC_DIR}/src/base.c
    ${JEMALLOC_DIR}/src/bin.c
    ${JEMALLOC_DIR}/src/bitmap.c
    ${JEMALLOC_DIR}/src/ckh.c
    ${JEMALLOC_DIR}/src/ctl.c
    ${JEMALLOC_DIR}/src/div.c
    ${JEMALLOC_DIR}/src/extent.c
    ${JEMALLOC_DIR}/src/extent_dss.c
    ${JEMALLOC_DIR}/src/extent_mmap.c
    ${JEMALLOC_DIR}/src/hash.c
    ${JEMALLOC_DIR}/src/hook.c
    ${JEMALLOC_DIR}/src/jemalloc.c
    ${JEMALLOC_DIR}/src/large.c
    ${JEMALLOC_DIR}/src/log.c
    ${JEMALLOC_DIR}/src/malloc_io.c
    ${JEMALLOC_DIR}/src/mutex.c
    ${JEMALLOC_DIR}/src/mutex_pool.c
    ${JEMALLOC_DIR}/src/nstime.c
    ${JEMALLOC_DIR}/src/pages.c
    ${JEMALLOC_DIR}/src/prng.c
    ${JEMALLOC_DIR}/src/prof.c
    ${JEMALLOC_DIR}/src/rtree.c
    ${JEMALLOC_DIR}/src/sc.c
    ${JEMALLOC_DIR}/src/stats.c
    ${JEMALLOC_DIR}/src/sz.c
    ${JEMALLOC_DIR}/src/tcache.c
    ${JEMALLOC_DIR}/src/test_hooks.c
    ${JEMALLOC_DIR}/src/ticker.c
    ${JEMALLOC_DIR}/src/tsd.c
    ${JEMALLOC_DIR}/src/witness.c
    ${JEMALLOC_DIR}/src/safety_check.c
)

add_library(jemalloc ${JEMALLOC_SRCS})
target_include_directories(jemalloc PUBLIC "${JEMALLOC_DIR}/include")

find_package(Threads REQUIRED)
target_link_libraries(jemalloc PUBLIC Threads::Threads)

target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

if (CMAKE_BUILD_TYPE STREQUAL "DEBUG")
    target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_DEBUG=1 -DJEMALLOC_PROF=1)
endif()

target_compile_options(jemalloc PRIVATE -Wno-redundant-decls)
# for RTLD_NEXT
target_compile_definitions(jemalloc PRIVATE _GNU_SOURCE)

set_property(TARGET jemalloc APPEND PROPERTY INTERFACE_COMPILE_DEFINITIONS USE_JEMALLOC=1)
