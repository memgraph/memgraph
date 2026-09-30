cmake_minimum_required(VERSION 3.15)
project(test_package LANGUAGES CXX)

add_executable(${PROJECT_NAME} test_package.cpp)

if (NURAFT_VERSION VERSION_LESS "3.0.0")
    find_package(nuraft CONFIG REQUIRED)
    target_link_libraries(${PROJECT_NAME} PRIVATE nuraft::nuraft)
else()
    find_package(NuRaft CONFIG REQUIRED)
    target_link_libraries(${PROJECT_NAME} PRIVATE $<IF:$<TARGET_EXISTS:NuRaft::static_lib>,NuRaft::static_lib,NuRaft::shared_lib>)
endif()
