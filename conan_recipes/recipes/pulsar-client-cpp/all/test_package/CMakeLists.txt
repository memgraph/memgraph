cmake_minimum_required(VERSION 3.15)
project(PackageTest CXX)


find_package(pulsar-client-cpp CONFIG REQUIRED)
add_executable(example src/example.cpp)
target_link_libraries(example pulsar-client-cpp::pulsar-client-cpp)
