set(stats_src_files
    metrics.cpp
    stats.cpp)

set(lcp_exe ${CMAKE_SOURCE_DIR}/tools/lcp)
set(lcp_src_files ../lisp/lcp.lisp ${lcp_exe})

# Use this function to add each lcp file to generation. This way each file is
# standalone and we avoid recompiling everything.
#
# You may pass a CAPNP_SCHEMA <id> keyword argument to generate the Cap'n Proto
# serialization code from .lcp file. You still need to add the generated capnp
# file through `add_capnp` function. To generate the <id> use `capnp id`
# invocation, and specify it here. This preserves correct id information across
# multiple schema generations. If this wasn't the case, wrong typeId
# information will break RPC between different compilations of memgraph.
#
# NOTE: stats_src_files and stats_lcp_files are globally updated.
# TODO: This is duplicated from src/CMakeLists.txt,
# find a good way to generalize this on per
# subdirectory basis.
function(add_lcp lcp_file)
  set(one_value_kwargs CAPNP_SCHEMA)
  cmake_parse_arguments(KW "" "${one_value_kwargs}" "" ${ARGN})
  string(REGEX REPLACE "\.lcp$" ".hpp" h_file
         "${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}")
  if (KW_CAPNP_SCHEMA)
    string(REGEX REPLACE "\.lcp$" ".capnp" capnp_file
           "${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}")
    set(capnp_id ${KW_CAPNP_SCHEMA})
    set(capnp_depend capnproto-proj)
    set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${lcp_file}.cpp)
    # Update *global* stats_src_files
    set(stats_src_files ${stats_src_files} ${cpp_file} PARENT_SCOPE)
  endif()
  add_custom_command(OUTPUT ${h_file} ${cpp_file} ${capnp_file}
    COMMAND ${lcp_exe} ${lcp_file} ${capnp_id}
    VERBATIM
    DEPENDS ${lcp_file} ${lcp_src_files} ${capnp_depend}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* stats_lcp_files
  set(stats_lcp_files ${stats_lcp_files} ${h_file} ${cpp_file} ${capnp_file} PARENT_SCOPE)
endfunction(add_lcp)

# Use this function to add each capnp file to generation. This way each file is
# standalone and we avoid recompiling everything.
# NOTE: stats_src_files and stats_capnp_files are globally updated.
# TODO: This is duplicated from src/CMakeLists.txt and
# src/utils/CMakeLists.txt, find a good way to generalize this on per
# subdirectory basis.
function(add_capnp capnp_src_file)
  set(cpp_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.c++)
  set(h_file ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file}.h)
  add_custom_command(OUTPUT ${cpp_file} ${h_file}
    COMMAND ${CAPNP_EXE} compile -o${CAPNP_CXX_EXE} ${capnp_src_file} -I ${CMAKE_CURRENT_SOURCE_DIR}/..
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${capnp_src_file} capnproto-proj
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
  # Update *global* stats_capnp_files
  set(stats_capnp_files ${stats_capnp_files} ${cpp_file} ${h_file} PARENT_SCOPE)
  # Update *global* stats_src_files
  set(stats_src_files ${stats_src_files} ${cpp_file} PARENT_SCOPE)
endfunction(add_capnp)

add_lcp(stats_rpc_messages.lcp CAPNP_SCHEMA @0xc19a87c81b9b4512)
add_capnp(stats_rpc_messages.capnp)

add_custom_target(generate_stats_lcp DEPENDS ${stats_lcp_files})
add_custom_target(generate_stats_capnp DEPENDS generate_stats_lcp ${stats_capnp_files})

add_library(mg-stats STATIC ${stats_src_files})
target_link_libraries(mg-stats Threads::Threads mg-utils mg-io mg-communication fmt glog gflags)
target_link_libraries(mg-stats capnp kj)
add_dependencies(mg-stats generate_stats_capnp)
