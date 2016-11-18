if (CMAKE_VERSION VERSION_LESS 3.2)
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "")
else()
    set(UPDATE_DISCONNECTED_IF_AVAILABLE "UPDATE_DISCONNECTED 1")
endif()

include(DownloadProject/DownloadProject)

download_project(
    PROJ googlebenchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG master
    ${UPDATE_DISCONNECTED_IF_AVAILABLE}
)

add_subdirectory(${googlebenchmark_SOURCE_DIR} ${googlebenchmark_BINARY_DIR})

include_directories("${googlebenchmark_SOURCE_DIR}/include")
