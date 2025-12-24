# Nitin A. Gawande, PNNL
# Oct 19, 2018

# set variables using nomenclature used in Makefile
set(MODULE_DIR community_detection_module/grappolo)
set(COFOLDER BasicCommunitiesDetection)
set(UTFOLDER Utility)
set(CLFOLDER Coloring)
set(FSFOLDER FullSyncOptimization)
set(DEFINSTR DefineStructure)

set(IO_HEADERS ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${DEFINSTR})
set(GRAPPOLO_HEADERS ${CMAKE_SOURCE_DIR}/${MODULE_DIR}/${DEFINSTR} PARENT_SCOPE)

add_subdirectory(BasicCommunitiesDetection)
add_subdirectory(Utility)
add_subdirectory(Coloring)
add_subdirectory(FullSyncOptimization)
