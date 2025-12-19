# grappolo


set( CLFOLDER_SRC
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/coloringDistanceOne.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/coloringMultiHashMaxMin.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/coloringUtils.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/equitableColoringDistanceOne.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/parallelLouvainWithColoring.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/parallelLouvainWithColoringNoMap.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/runMultiPhaseColoring.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${CLFOLDER}/vBase.cpp
   )

add_library( coloring STATIC
  ${CLFOLDER_SRC}
)

target_include_directories( coloring PUBLIC ${IO_HEADERS} )

target_link_libraries(coloring PRIVATE mg_utility)

install( TARGETS
  coloring
  DESTINATION lib
)

install( FILES
  ${IO_HEADERS}/defs.h
  ${IO_HEADERS}/coloring.h
  ${IO_HEADERS}/coloringUtils.h
  ${IO_HEADERS}/utilityClusteringFunctions.h
  ${IO_HEADERS}/color_comm.h
  ${IO_HEADERS}/basic_comm.h
  DESTINATION include
)

