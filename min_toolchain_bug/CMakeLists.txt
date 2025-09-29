cmake_minimum_required(VERSION 4.0)
project(bla)

add_executable(main src/main.cpp)
find_package(Boost)
target_link_libraries(main PRIVATE Boost::iostreams)
