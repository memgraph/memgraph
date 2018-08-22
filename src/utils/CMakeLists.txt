set(utils_src_files
    demangle.cpp
    file.cpp
    signals.cpp
    thread.cpp
    thread/sync.cpp
    uuid.cpp
    watchdog.cpp)

# Use this function to add each capnp file to generation. This way each file is
# standalone and we avoid recompiling everything.
# NOTE: utils_src_files and utils_capnp_files are globally updated.
# TODO: This is duplicated from src/CMakeLists.txt,
# find a good way to generalize this on per
# subdirectory basis.
function(add_capnp capnp_src_file)
  set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.c++)
  set(h_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.h)
  add_custom_command(OUTPUT ${cpp_file} ${h_file}
    COMMAND ${CAPNP_EXE} compile -o${CAPNP_CXX_EXE} ${capnp_src_file} -I ${CMAKE_CURRENT_SOURCE_DIR}/..
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file} capnproto-proj
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* stats_capnp_files
  set(utils_capnp_files ${utils_capnp_files} ${cpp_file} ${h_file} PARENT_SCOPE)
  # Update *global* stats_src_files
  set(utils_src_files ${utils_src_files} ${cpp_file} PARENT_SCOPE)
endfunction(add_capnp)

add_capnp(serialization.capnp)

add_library(mg-utils STATIC ${utils_src_files})
target_link_libraries(mg-utils stdc++fs Threads::Threads fmt glog gflags uuid)
target_link_libraries(mg-utils capnp kj)
