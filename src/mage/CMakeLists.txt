# MAGE: Memgraph Advanced Graph Extensions.
#
# Gated by MG_BUILD_MAGE at the root. Produces the complete set of MAGE
# query modules (C++, Python, Rust) in ${MG_MAGE_DIST_DIR} for local use
# (point memgraph at it with --query-modules-directory) and installs them
# under the `mage` / `mage_debuginfo` components for packaging.

# Any install() below that doesn't say otherwise belongs to the mage package.
set(CMAKE_INSTALL_DEFAULT_COMPONENT_NAME "mage")

# Everything a MAGE deployment ships lands here, mirroring the flat layout
# the old standalone `mage/setup` assembled in mage/dist. Pinned to
# build/mage/dist (not the source-mirrored build/src/mage/) so the
# dev-facing dist location stays stable regardless of where the sources
# live in the tree.
set(MG_MAGE_DIST_DIR "${CMAKE_BINARY_DIR}/mage/dist" CACHE INTERNAL
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

# Python dependency manifest + installer, consumed by the mage package's
# post-install scriptlet (deb postinst / rpm %post run
# install_python_requirements.sh against mage-requirements.txt). GPU builds
# ship the requirements-gpu manifest instead.
if(MG_ENABLE_CUGRAPH)
    set(_mage_requirements ${CMAKE_CURRENT_SOURCE_DIR}/python/requirements-gpu.txt)
else()
    set(_mage_requirements ${CMAKE_CURRENT_SOURCE_DIR}/python/requirements.txt)
endif()
install(FILES ${_mage_requirements}
        DESTINATION lib/memgraph
        RENAME mage-requirements.txt
        COMPONENT mage)
install(PROGRAMS ${CMAKE_SOURCE_DIR}/release/package/mage/install_python_requirements.sh
        DESTINATION lib/memgraph
        COMPONENT mage)

# Aggregate target: `ninja mage` builds every MAGE artifact.
get_property(_mage_module_targets GLOBAL PROPERTY MG_MAGE_MODULE_TARGETS)
add_custom_target(mage DEPENDS
    ${_mage_module_targets}
    mage_python_modules
    mage_rust_modules
    mage_grappolo_license)
