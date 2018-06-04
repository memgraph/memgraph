set(io_src_files
    network/addrinfo.cpp
    network/endpoint.cpp
    network/socket.cpp
    network/utils.cpp)

# Use this function to add each capnp file to generation. This way each file is
# standalone and we avoid recompiling everything.
# NOTE: io_src_files and io_capnp_files are globally updated.
# TODO: This is duplicated from src/CMakeLists.txt, find a good way to
# generalize this on per subdirectory basis.
function(add_capnp capnp_src_file)
  set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.c++)
  set(h_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.h)
  add_custom_command(OUTPUT ${cpp_file} ${h_file}
    COMMAND ${CAPNP_EXE} compile -o${CAPNP_CXX_EXE} ${capnp_src_file} -I ${CMAKE_CURRENT_SOURCE_DIR}
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file} capnproto-proj
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* io_capnp_files
  set(io_capnp_files ${io_capnp_files} ${cpp_file} ${h_file} PARENT_SCOPE)
  # Update *global* io_src_files
  set(io_src_files ${io_src_files} ${cpp_file} PARENT_SCOPE)
endfunction(add_capnp)

add_capnp(network/endpoint.capnp)

add_custom_target(generate_io_capnp DEPENDS ${io_capnp_files})

add_library(mg-io STATIC ${io_src_files})
target_link_libraries(mg-io stdc++fs Threads::Threads fmt glog mg-utils)
target_link_libraries(mg-io capnp kj)
add_dependencies(mg-io generate_io_capnp)
