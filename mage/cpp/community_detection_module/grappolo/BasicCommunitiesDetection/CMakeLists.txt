# grappolo


set( COFOLDER_SRC
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodApprox-2.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodApprox.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethod.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodFastTrackResistance.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodNoMap.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodNoMapFastTrackResistance.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodScale.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodScaleFastTrackResistance.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/runMultiPhaseBasicApprox.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/runMultiPhaseBasic.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${COFOLDER}/runMultiPhaseBasicFastTrackResistance.cpp
   )


add_library( basic_cd
  ${COFOLDER_SRC}
)

target_include_directories(basic_cd PUBLIC ${IO_HEADERS} )

target_link_libraries(basic_cd PRIVATE mg_utility)

install( TARGETS
  basic_cd
  DESTINATION lib
)

install( FILES
  ${IO_HEADERS}/defs.h
  ${IO_HEADERS}/utilityClusteringFunctions.h
  ${IO_HEADERS}/basic_comm.h
  ${IO_HEADERS}/basic_util.h
  DESTINATION include
)

