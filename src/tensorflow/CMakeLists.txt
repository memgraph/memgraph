cmake_minimum_required(VERSION 2.8)

set(communication_src_files
    ${CMAKE_SOURCE_DIR}/src/communication/bolt/v1/value.cpp
    ${CMAKE_SOURCE_DIR}/src/communication/buffer.cpp
    ${CMAKE_SOURCE_DIR}/src/communication/client.cpp
    ${CMAKE_SOURCE_DIR}/src/communication/context.cpp
    ${CMAKE_SOURCE_DIR}/src/communication/helpers.cpp
    ${CMAKE_SOURCE_DIR}/src/communication/init.cpp)

set(utils_src_files
    ${CMAKE_SOURCE_DIR}/src/utils/demangle.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/file.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/signals.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/thread.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/thread/sync.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/uuid.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/watchdog.cpp)

set(io_src_files
    ${CMAKE_SOURCE_DIR}/src/io/network/addrinfo.cpp
    ${CMAKE_SOURCE_DIR}/src/io/network/endpoint.cpp
    ${CMAKE_SOURCE_DIR}/src/io/network/socket.cpp
    ${CMAKE_SOURCE_DIR}/src/io/network/utils.cpp)

#See https://www.tensorflow.org/how_tos/adding_an_op/
execute_process(COMMAND python3 -c "import tensorflow; print(tensorflow.sysconfig.get_include())" OUTPUT_VARIABLE Tensorflow_INCLUDE_DIRS)
execute_process(COMMAND python3 -c "import tensorflow; print(tensorflow.sysconfig.get_lib() + '/libtensorflow_framework.so', end='')" OUTPUT_VARIABLE Tensorflow_INCLUDE_LIB)

add_library(memgraph_op SHARED memgraph_op.cc ${communication_src_files} ${utils_src_files} ${io_src_files})
target_include_directories(memgraph_op SYSTEM PUBLIC ${Tensorflow_INCLUDE_DIRS})

target_link_libraries(memgraph_op ${Tensorflow_INCLUDE_LIB} mg-utils)
target_link_libraries(memgraph_op stdc++fs Threads::Threads fmt glog gflags uuid)
target_link_libraries(memgraph_op Threads::Threads mg-io fmt glog gflags)
target_link_libraries(memgraph_op ${OPENSSL_LIBRARIES})
target_include_directories(memgraph_op SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})

target_compile_definitions(memgraph_op PUBLIC -D_GLIBCXX_USE_CXX11_ABI=0)
