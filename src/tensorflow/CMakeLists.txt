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

# get tensorflow include dirs, see https://www.tensorflow.org/how_tos/adding_an_op/
execute_process(COMMAND python3 -c "import tensorflow; print(tensorflow.sysconfig.get_include())" OUTPUT_VARIABLE Tensorflow_INCLUDE_DIRS)

#target_link_libraries(mg-communication)

# C++11 required for tensorflow
#set(CMAKE_CXX_FLAGS "-std=c++11 -D_GLIBCXX_USE_CXX11_ABI=0 -L/home/dino/.local/lib/python3.5/site-packages/tensorflow/ -ltensorflow_framework ${CMAKE_CXX_FLAGS}")

add_library(bolt_wrapper SHARED bolt_wrapper.cc ${communication_src_files} ${utils_src_files} ${io_src_files})
target_include_directories(bolt_wrapper SYSTEM PUBLIC ${Tensorflow_INCLUDE_DIRS})

target_link_libraries(bolt_wrapper /home/dino/.local/lib/python3.5/site-packages/tensorflow/libtensorflow_framework.so mg-utils)
target_link_libraries(bolt_wrapper stdc++fs Threads::Threads fmt glog gflags uuid)
target_link_libraries(bolt_wrapper Threads::Threads mg-io fmt glog gflags)
target_link_libraries(bolt_wrapper ${OPENSSL_LIBRARIES})
target_include_directories(bolt_wrapper SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})

target_compile_definitions(bolt_wrapper PUBLIC -D_GLIBCXX_USE_CXX11_ABI=0)

### ###


