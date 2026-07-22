# grappolo

set( UTFOLDER_SRC
     ${MODULE_DIR}/${UTFOLDER}/buildNextPhase.cpp
     ${MODULE_DIR}/${UTFOLDER}/reverseCuthillMcKee.cpp
     ${MODULE_DIR}/${UTFOLDER}/RngStream.cpp
     ${MODULE_DIR}/${UTFOLDER}/utilityClusterComparisonMetrics.cpp
     ${MODULE_DIR}/${UTFOLDER}/utilityClusteringFunctions.cpp
     ${MODULE_DIR}/${UTFOLDER}/utilityDataStructures.cpp
     ${MODULE_DIR}/${UTFOLDER}/utilityFunctions.cpp
     ${MODULE_DIR}/${UTFOLDER}/utilitySparsificationFunctions.cpp
     ${MODULE_DIR}/${UTFOLDER}/vertexFollowing.cpp
   )

add_library( util STATIC
  ${UTFOLDER_SRC}
)

target_include_directories( util PUBLIC ${IO_HEADERS} )

target_link_libraries( util PRIVATE mg_utility )

