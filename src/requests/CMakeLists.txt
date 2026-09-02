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
        PRIVATE spdlog::spdlog fmt::fmt gflags CURL::libcurl)
