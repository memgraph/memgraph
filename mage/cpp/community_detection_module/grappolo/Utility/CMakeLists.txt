# grappolo

set( UTFOLDER_SRC
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/buildNextPhase.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/reverseCuthillMcKee.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/RngStream.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/utilityClusterComparisonMetrics.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/utilityClusteringFunctions.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/utilityDataStructures.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/utilityFunctions.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/utilitySparsificationFunctions.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${UTFOLDER}/vertexFollowing.cpp
   )

add_library( util STATIC
  ${UTFOLDER_SRC}
)

target_include_directories( util PUBLIC ${IO_HEADERS} )

target_link_libraries( util PRIVATE mg_utility )

install( TARGETS
  util
  DESTINATION lib
)

install( FILES
  ${IO_HEADERS}/defs.h
  ${IO_HEADERS}/basic_util.h
  ${IO_HEADERS}/utilityClusteringFunctions.h
  ${IO_HEADERS}/RngStream.h
  ${IO_HEADERS}/basic_comm.h
  DESTINATION include
)

