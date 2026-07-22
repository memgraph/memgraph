# grappolo

set( COFOLDER_SRC
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodApprox-2.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodApprox.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethod.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodFastTrackResistance.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodNoMap.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodNoMapFastTrackResistance.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodScale.cpp
     ${MODULE_DIR}/${COFOLDER}/parallelLouvainMethodScaleFastTrackResistance.cpp
     ${MODULE_DIR}/${COFOLDER}/runMultiPhaseBasicApprox.cpp
     ${MODULE_DIR}/${COFOLDER}/runMultiPhaseBasic.cpp
     ${MODULE_DIR}/${COFOLDER}/runMultiPhaseBasicFastTrackResistance.cpp
   )

add_library( basic_cd
  ${COFOLDER_SRC}
)

target_include_directories(basic_cd PUBLIC ${IO_HEADERS} )

target_link_libraries(basic_cd PRIVATE mg_utility)

