# grappolo

set( FSFOLDER_SRC
     ${MODULE_DIR}/${FSFOLDER}/fullSyncUtility.cpp
     ${MODULE_DIR}/${FSFOLDER}/parallelLouvainMethodEarlyTerminate.cpp
     ${MODULE_DIR}/${FSFOLDER}/parallelLouvainMethodFullSync.cpp
     ${MODULE_DIR}/${FSFOLDER}/parallelLouvainMethodFullSyncEarly.cpp
     ${MODULE_DIR}/${FSFOLDER}/runMultiPhaseSyncType.cpp
   )

add_library( full_syn_opt STATIC
  ${FSFOLDER_SRC}
)

target_include_directories( full_syn_opt PUBLIC ${IO_HEADERS} )

target_link_libraries( full_syn_opt PRIVATE mg_utility)

