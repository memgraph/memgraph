set(requests_src_files
    requests.cpp)

find_package(CURL REQUIRED)
find_package(fmt REQUIRED)
find_package(gflags REQUIRED)
find_package(spdlog REQUIRED)
find_package(ctre REQUIRED)


add_library(mg-requests STATIC ${requests_src_files})
target_link_libraries(mg-requests
        PUBLIC mg-utils nlohmann_json::nlohmann_json ctre::ctre
        # mg-flags provides FLAGS_ca_bundle_file; the static-lib cycle with
        # mg-flags -> mg-license -> mg-requests is fine (CMake repeats archives).
        PRIVATE spdlog::spdlog fmt::fmt gflags CURL::libcurl mg-flags)
