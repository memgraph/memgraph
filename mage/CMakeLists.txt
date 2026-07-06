# MAGE: Memgraph Advanced Graph Extensions.
#
# Gated by MG_BUILD_MAGE at the root. Produces the complete set of MAGE
# query modules (C++, Python, Rust) in ${MG_MAGE_DIST_DIR} for local use
# (point memgraph at it with --query-modules-directory) and installs them
# under the `mage` / `mage_debuginfo` components for packaging.

# Any install() below that doesn't say otherwise belongs to the mage package.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "mage")

# Everything a MAGE deployment ships lands here, mirroring the flat layout
# the old standalone `mage/setup` assembled in mage/dist.
set(MG_MAGE_DIST_DIR "${CMAKE_CURRENT_BINARY_DIR}/dist" CACHE INTERNAL
    "Directory where all built MAGE query modules are collected")
set(MG_MAGE_INSTALL_DIR "lib/memgraph/query_modules")
file(MAKE_DIRECTORY ${MG_MAGE_DIST_DIR})

add_subdirectory(cpp)
add_subdirectory(python)
add_subdirectory(rust)

# The Louvain community detection module embeds grappolo; its license must
# ship next to the modules.
set(_grappolo_license_dir ${CMAKE_SOURCE_DIR}/licenses/third-party/grappolo)
mg_add_stamped_custom_target(mage_grappolo_license ALL
    STAMP_DIR ${MG_MAGE_DIST_DIR}
    COMMANDS
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${_grappolo_license_dir} ${MG_MAGE_DIST_DIR}/grappolo
    DEPENDS ${_grappolo_license_dir}/LICENSE
    COMMENT "Copying grappolo license into MAGE dist")
install(DIRECTORY ${_grappolo_license_dir}
        DESTINATION ${MG_MAGE_INSTALL_DIR} COMPONENT mage)

# Aggregate target: `ninja mage` builds every MAGE artifact.
get_property(_mage_module_targets GLOBAL PROPERTY MG_MAGE_MODULE_TARGETS)
add_custom_target(mage DEPENDS
    ${_mage_module_targets}
    mage_python_modules
    mage_rust_modules
    mage_grappolo_license)
