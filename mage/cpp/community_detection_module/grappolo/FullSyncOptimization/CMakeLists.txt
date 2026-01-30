# grappolo

set( FSFOLDER_SRC
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${FSFOLDER}/fullSyncUtility.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${FSFOLDER}/parallelLouvainMethodEarlyTerminate.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${FSFOLDER}/parallelLouvainMethodFullSync.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${FSFOLDER}/parallelLouvainMethodFullSyncEarly.cpp
     ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${FSFOLDER}/runMultiPhaseSyncType.cpp
   )

add_library( full_syn_opt STATIC
  ${FSFOLDER_SRC}
)

target_include_directories( full_syn_opt PUBLIC ${IO_HEADERS} )

target_link_libraries( full_syn_opt PRIVATE mg_utility)

install( TARGETS
  full_syn_opt
  DESTINATION lib
)

install( FILES
  ${IO_HEADERS}/defs.h
  ${IO_HEADERS}/utilityClusteringFunctions.h
  ${IO_HEADERS}/sync_comm.h
  DESTINATION include
)

