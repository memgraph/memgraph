# grappolo

set( CLFOLDER_SRC
     ${MODULE_DIR}/${CLFOLDER}/coloringDistanceOne.cpp
     ${MODULE_DIR}/${CLFOLDER}/coloringMultiHashMaxMin.cpp
     ${MODULE_DIR}/${CLFOLDER}/coloringUtils.cpp
     ${MODULE_DIR}/${CLFOLDER}/equitableColoringDistanceOne.cpp
     ${MODULE_DIR}/${CLFOLDER}/parallelLouvainWithColoring.cpp
     ${MODULE_DIR}/${CLFOLDER}/parallelLouvainWithColoringNoMap.cpp
     ${MODULE_DIR}/${CLFOLDER}/runMultiPhaseColoring.cpp
     ${MODULE_DIR}/${CLFOLDER}/vBase.cpp
   )

add_library( coloring STATIC
  ${CLFOLDER_SRC}
)

target_include_directories( coloring PUBLIC ${IO_HEADERS} )

target_link_libraries(coloring PRIVATE mg_utility)

