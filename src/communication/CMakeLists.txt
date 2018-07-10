set(communication_src_files
    bolt/v1/decoder/decoded_value.cpp
    buffer.cpp
    client.cpp
    context.cpp
    helpers.cpp
    init.cpp
    rpc/client.cpp
    rpc/protocol.cpp
    rpc/server.cpp)

# TODO: Extract data_structures to library
set(communication_src_files ${communication_src_files}
    ${CMAKE_SOURCE_DIR}/src/data_structures/concurrent/skiplist_gc.cpp)

# Use this function to add each capnp file to generation. This way each file is
# standalone and we avoid recompiling everything.
# NOTE: communication_src_files and communication_capnp_files are globally updated.
# TODO: This is duplicated from src/CMakeLists.txt and
# src/utils/CMakeLists.txt, find a good way to generalize this on per
# subdirectory basis.
function(add_capnp capnp_src_file)
  set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.c++)
  set(h_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.h)
  add_custom_command(OUTPUT ${cpp_file} ${h_file}
    COMMAND ${CAPNP_EXE} compile -o${CAPNP_CXX_EXE} ${capnp_src_file} -I ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file} capnproto-proj
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* communication_capnp_files
  set(communication_capnp_files ${communication_capnp_files} ${cpp_file} ${h_file} PARENT_SCOPE)
  # Update *global* communication_src_files
  set(communication_src_files ${communication_src_files} ${cpp_file} PARENT_SCOPE)
endfunction(add_capnp)

add_capnp(rpc/messages.capnp)

add_custom_target(generate_communication_capnp DEPENDS ${communication_capnp_files})

add_library(mg-communication STATIC ${communication_src_files})
target_link_libraries(mg-communication Threads::Threads mg-utils mg-io fmt glog gflags)
target_link_libraries(mg-communication ${OPENSSL_LIBRARIES})
target_include_directories(mg-communication SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
target_link_libraries(mg-communication capnp kj)
add_dependencies(mg-communication generate_communication_capnp)
