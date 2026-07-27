# Nitin A. Gawande, PNNL
# Oct 19, 2018

# set variables using nomenclature used in Makefile
# MODULE_DIR is an absolute path so subdirectory source lists resolve the same
# whether this tree is configured standalone or under the memgraph root.
set(MODULE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
set(COFOLDER BasicCommunitiesDetection)
set(UTFOLDER Utility)
set(CLFOLDER Coloring)
set(FSFOLDER FullSyncOptimization)
set(DEFINSTR DefineStructure)

set(IO_HEADERS ${MODULE_DIR}/${DEFINSTR})
set(GRAPPOLO_HEADERS ${MODULE_DIR}/${DEFINSTR} PARENT_SCOPE)

add_subdirectory(BasicCommunitiesDetection)
add_subdirectory(Utility)
add_subdirectory(Coloring)
add_subdirectory(FullSyncOptimization)
