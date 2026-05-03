# Install-time helper: place a symlink at
#   ${CMAKE_INSTALL_PREFIX}/lib/debug/.build-id/<aa>/<rest>.dwp
# pointing at lib/memgraph/memgraph.dwp, so build-id-keyed resolvers (gdb,
# debuginfod) and standard /usr/lib/debug/.build-id/ paths find the .dwp.
#
# Reads the binary's .note.gnu.build-id via readelf. Honors DESTDIR.
# No-op if either the binary or the .dwp is missing in the staging tree.

set(_root "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}")
set(_dwp "${_root}/lib/memgraph/memgraph.dwp")
set(_bin "${_root}/lib/memgraph/memgraph")

if(NOT EXISTS "${_dwp}" OR NOT EXISTS "${_bin}")
    return()
endif()

execute_process(COMMAND readelf -n "${_bin}"
                OUTPUT_VARIABLE _notes
                RESULT_VARIABLE _rc)
if(NOT _rc EQUAL 0)
    message(WARNING "readelf failed on ${_bin}; skipping build-id-keyed .dwp link")
    return()
endif()

string(REGEX MATCH "Build ID: ([0-9a-f]+)" _ "${_notes}")
set(_id "${CMAKE_MATCH_1}")
if(NOT _id)
    message(WARNING "No build-id found in ${_bin}; skipping build-id-keyed .dwp link")
    return()
endif()

string(SUBSTRING "${_id}" 0 2 _aa)
string(SUBSTRING "${_id}" 2 -1 _rest)
set(_link_dir "${_root}/lib/debug/.build-id/${_aa}")
file(MAKE_DIRECTORY "${_link_dir}")
execute_process(COMMAND ${CMAKE_COMMAND} -E create_symlink
                "../../../memgraph/memgraph.dwp"
                "${_link_dir}/${_rest}.dwp")
message(STATUS "Linked build-id-keyed .dwp: ${_link_dir}/${_rest}.dwp")
