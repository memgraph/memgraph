# Install systemd service (must use absolute path).
if(MG_BUILD_MEMGRAPH)
    install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/memgraph.service
        DESTINATION /lib/systemd/system)
endif()

# Set parameters to recognize the host distro
cmake_host_system_information(RESULT DISTRO QUERY DISTRIB_NAME)
cmake_host_system_information(RESULT DISTRO_VERSION QUERY DISTRIB_VERSION)

# ---- Setup CPack --------

# General setup
set(CPACK_PACKAGE_NAME memgraph)
set(CPACK_PACKAGE_VENDOR "Memgraph Ltd.")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "High performance, in-memory, transactional graph database")

# Setting architecture extension for deb packages
set(MG_ARCH_EXTENSION_DEB "all")

if(${MG_ARCH} STREQUAL "x86_64")
    set(MG_ARCH_EXTENSION_DEB "amd64")
elseif(${MG_ARCH} STREQUAL "ARM64")
    set(MG_ARCH_EXTENSION_DEB "arm64")
endif()

# DEB specific
# Instead of using "name <email>" format, we use "email (name)" to prevent
# errors due to full stop, '.' at the end of "Ltd". (See: RFC 822)
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "tech@memgraph.com (Memgraph Ltd.)")
set(CPACK_DEBIAN_PACKAGE_SECTION non-free/database)
set(CPACK_DEBIAN_PACKAGE_HOMEPAGE https://memgraph.com)
set(CPACK_DEBIAN_PACKAGE_VERSION "${MEMGRAPH_VERSION_DEB}")
set(CPACK_DEBIAN_MEMGRAPH_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/conffiles;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/copyright;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/preinst;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/prerm;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postrm;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postinst;")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)

# Description formatting is important, summary must be followed with a newline and 1 space.
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}
 Contains Memgraph, the graph database. It aims to deliver developers the
 speed, simplicity and scale required to build the next generation of
 applications driver by real-time connected data.")

# Add `openssl` package to dependencies list. Used to generate SSL certificates.
# We also depend on `python3` because we embed it in Memgraph.
set(CPACK_DEBIAN_PACKAGE_DEPENDS "openssl (>= 1.1.0), python3 (>= 3.5.0)")

# Setting architecture extension for rpm packages
set(MG_ARCH_EXTENSION_RPM "noarch")

if(${MG_ARCH} STREQUAL "x86_64")
    set(MG_ARCH_EXTENSION_RPM "x86_64")
elseif(${MG_ARCH} STREQUAL "ARM64")
    set(MG_ARCH_EXTENSION_RPM "aarch64")
endif()

# RPM specific
set(CPACK_RPM_PACKAGE_URL https://memgraph.com)
set(CPACK_RPM_PACKAGE_VERSION "${MEMGRAPH_VERSION_RPM}")
set(CPACK_RPM_MEMGRAPH_FILE_NAME "memgraph-${MEMGRAPH_VERSION_RPM}-1.${MG_ARCH_EXTENSION_RPM}.rpm")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    /var /var/lib /var/log /etc/logrotate.d
    /lib /lib/systemd /lib/systemd/system /lib/systemd/system/memgraph.service)
set(CPACK_RPM_MEMGRAPH_PACKAGE_REQUIRES_PRE "shadow-utils")
set(CPACK_RPM_MEMGRAPH_USER_BINARY_SPECFILE "${CMAKE_CURRENT_SOURCE_DIR}/rpm/memgraph.spec.in")
set(CPACK_RPM_PACKAGE_LICENSE "Memgraph License")
# CPACK deduces dependency to libstdc++ which:
#   * can't be easily installed on Centos 7 (the one from the toolchain,
#     only required to avoid printing issue within query modules)
#   * it causes issues with glibcxx 2.4
#     `if(DISTRO STREQUAL "Amazon Linux" AND DISTRO_VERSION STREQUAL "2")`
set(CPACK_RPM_PACKAGE_AUTOREQ " no")

# Description formatting is important, no line must be greater than 80 characters.
set(CPACK_RPM_PACKAGE_DESCRIPTION "Contains Memgraph, the graph database.
It aims to deliver developers the speed, simplicity and scale required to build
the next generation of applications driver by real-time connected data.")

# Add `openssl` package to dependencies list. Used to generate SSL certificates.
# We also depend on `python3` because we embed it in Memgraph. `libatomic` is a
# runtime dependency of the binary; since CPACK_RPM_PACKAGE_AUTOREQ is off
# (above), soname deps aren't auto-detected and must be listed explicitly.
set(CPACK_RPM_MEMGRAPH_PACKAGE_REQUIRES "openssl >= 1.0.0, curl >= 7.29.0, python3 >= 3.5.0, logrotate, libatomic")

# When memgraph is built against an explicit Python (currently CentOS Stream 9,
# where it embeds python 3.12 instead of the distro-default 3.9 — see
# -DMG_PYTHON_VERSION), require that interpreter's package so the embedded
# libpython is present and the MAGE %post (which runs python<ver> -m pip) works.
if(NOT "${MG_PYTHON_VERSION}" STREQUAL "")
    set(CPACK_RPM_MEMGRAPH_PACKAGE_REQUIRES "${CPACK_RPM_MEMGRAPH_PACKAGE_REQUIRES}, python${MG_PYTHON_VERSION}")
endif()

# Configure CPack to build mgconsole using the unified install script.
if(MG_BUILD_MEMGRAPH)
    set(CPACK_INSTALL_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/install_mgconsole.cmake")
endif()

# Component-based packaging. Depending on MG_BUILD_MEMGRAPH / MG_BUILD_MAGE,
# produces up to four packages (as DEB or RPM):
#   * memgraph_<ver>_<arch>.deb / memgraph-<ver>-1.<arch>.rpm
#       main package: binary, libstdc++, query modules, configs, systemd unit
#   * memgraph-debuginfo_<ver>_<arch>.deb / memgraph-debuginfo-<ver>-1.<arch>.rpm
#       companion: .debug sidecars for memgraph + query modules. Depends on
#       the main package at the exact matching version. .debug files install
#       alongside their binaries; gdb resolves them via .gnu_debuglink.
#   * memgraph-mage_<ver>_<arch>.deb / memgraph-mage-<ver>-1.<arch>.rpm
#       MAGE query modules + bundled toolchain libomp + python requirements
#   * memgraph-mage-debuginfo_* — .debug sidecars for the MAGE modules
#       (only when the build was configured with MG_SPLIT_DEBUG=ON)
set(CPACK_COMPONENTS_ALL)
if(MG_BUILD_MEMGRAPH)
    list(APPEND CPACK_COMPONENTS_ALL memgraph debuginfo)
endif()

# --- DEB component config ---
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_DEBIAN_MEMGRAPH_PACKAGE_NAME "memgraph")
set(CPACK_DEBIAN_MEMGRAPH_FILE_NAME
    "memgraph_${MEMGRAPH_VERSION_DEB}_${MG_ARCH_EXTENSION_DEB}.deb")

set(CPACK_DEBIAN_DEBUGINFO_PACKAGE_NAME "memgraph-debuginfo")
set(CPACK_DEBIAN_DEBUGINFO_FILE_NAME
    "memgraph-debuginfo_${MEMGRAPH_VERSION_DEB}_${MG_ARCH_EXTENSION_DEB}.deb")
set(CPACK_DEBIAN_DEBUGINFO_PACKAGE_DEPENDS
    "memgraph (= ${MEMGRAPH_VERSION_DEB})")
set(CPACK_DEBIAN_DEBUGINFO_PACKAGE_SECTION "debug")
set(CPACK_DEBIAN_DEBUGINFO_DESCRIPTION
    "Debug symbols (.debug sidecars) for the matching memgraph package.
 Install alongside memgraph for symbolicated stack traces; gdb auto-locates
 each sidecar via .gnu_debuglink next to the corresponding binary.")

# --- RPM component config ---
set(CPACK_RPM_COMPONENT_INSTALL ON)
set(CPACK_RPM_MEMGRAPH_PACKAGE_NAME "memgraph")
set(CPACK_RPM_DEBUGINFO_PACKAGE_NAME "memgraph-debuginfo")

# Both component specs are hand-written. The debuginfo spec disables
# rpmbuild's brp-strip so our pre-extracted .debug sidecars keep their
# DWARF; the main spec retains rpmbuild's default postprocessing.
set(CPACK_RPM_DEBUGINFO_USER_BINARY_SPECFILE "${CMAKE_CURRENT_SOURCE_DIR}/rpm/memgraph-debuginfo.spec.in")
set(CPACK_RPM_DEBUGINFO_FILE_NAME
    "memgraph-debuginfo-${MEMGRAPH_VERSION_RPM}-1.${MG_ARCH_EXTENSION_RPM}.rpm")
set(CPACK_RPM_DEBUGINFO_PACKAGE_REQUIRES
    "memgraph = ${MEMGRAPH_VERSION_RPM}-1")
set(CPACK_RPM_DEBUGINFO_PACKAGE_AUTOREQ "no")
set(CPACK_RPM_DEBUGINFO_PACKAGE_SUMMARY
    "Debug symbols for memgraph")
set(CPACK_RPM_DEBUGINFO_PACKAGE_DESCRIPTION
    "Debug symbols (.debug sidecars) for the matching memgraph package.")

# --- MAGE components (memgraph-mage / memgraph-mage-debuginfo) ---
if(MG_BUILD_MAGE)
    list(APPEND CPACK_COMPONENTS_ALL mage)

    # Values consumed by the package scriptlets (deb postinst, rpm %post),
    # which run install_python_requirements.sh at install time; it expects
    # --arch amd64|arm64. CPACK_-prefixed so they're serialized into
    # CPackConfig.cmake and available to the rpm spec template at cpack time.
    if(MG_ENABLE_CUGRAPH)
        set(CPACK_MAGE_CUDA "true")
    else()
        set(CPACK_MAGE_CUDA "false")
    endif()
    set(CPACK_MAGE_PKG_ARCH "${MG_ARCH_EXTENSION_DEB}")

    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/mage/postinst.in
        ${CMAKE_CURRENT_BINARY_DIR}/mage/postinst
        @ONLY
        FILE_PERMISSIONS
            OWNER_READ OWNER_WRITE OWNER_EXECUTE
            GROUP_READ GROUP_EXECUTE
            WORLD_READ WORLD_EXECUTE)

    # --- DEB ---
    set(CPACK_DEBIAN_MAGE_PACKAGE_NAME "memgraph-mage")
    set(CPACK_DEBIAN_MAGE_FILE_NAME
        "memgraph-mage_${MEMGRAPH_VERSION_DEB}_${MG_ARCH_EXTENSION_DEB}.deb")
    set(CPACK_DEBIAN_MAGE_PACKAGE_SECTION "database")
    # The dependency on memgraph is deliberately UNVERSIONED so daily and
    # release builds of the two packages can be mixed.
    # No libgomp1: the modules use the bundled toolchain libomp instead.
    # Dependencies are declared manually (mirroring the retired
    # tools/ci/mage-build/package/debian/control), so shlibdeps stays off —
    # it can't resolve the bundled libomp.so to a distro package anyway.
    set(CPACK_DEBIAN_MAGE_PACKAGE_DEPENDS
        "memgraph, libcurl4, libpython3.12, openssl, python3, python3-pip, python3-setuptools, adduser, libaio1t64, libatomic1, libdw1t64 | libdw1")
    set(CPACK_DEBIAN_MAGE_PACKAGE_SHLIBDEPS OFF)
    # Both memgraph and memgraph-mage ship the mage/ python package files
    # (see query_modules/CMakeLists.txt); dpkg needs Replaces to allow that.
    set(CPACK_DEBIAN_MAGE_PACKAGE_REPLACES "memgraph")
    set(CPACK_DEBIAN_MAGE_PACKAGE_CONTROL_EXTRA
        "${CMAKE_CURRENT_BINARY_DIR}/mage/postinst")
    set(CPACK_DEBIAN_MAGE_DESCRIPTION
        "Extra query modules for Memgraph
 Additional query modules for Memgraph, installed under
 /usr/lib/memgraph/query_modules/.")

    # --- RPM ---
    set(CPACK_RPM_MAGE_PACKAGE_NAME "memgraph-mage")
    set(CPACK_RPM_MAGE_FILE_NAME
        "memgraph-mage-${MEMGRAPH_VERSION_RPM}-1.${MG_ARCH_EXTENSION_RPM}.rpm")
    set(CPACK_RPM_MAGE_USER_BINARY_SPECFILE
        "${CMAKE_CURRENT_SOURCE_DIR}/rpm/memgraph-mage.spec.in")
    # Unversioned memgraph dep (see DEB comment). util-linux provides
    # runuser for the %post scriptlet. No libgomp (bundled libomp instead).
    set(CPACK_RPM_MAGE_PACKAGE_REQUIRES
        "memgraph, libcurl, openssl, python3, python3-pip, python3-setuptools, libatomic, util-linux")
    set(CPACK_RPM_MAGE_PACKAGE_AUTOREQ "no")
    set(CPACK_RPM_MAGE_PACKAGE_SUMMARY "Extra query modules for Memgraph")
    set(CPACK_RPM_MAGE_PACKAGE_DESCRIPTION
        "Additional query modules for Memgraph, installed under
/usr/lib/memgraph/query_modules/.")

    if(MG_SPLIT_DEBUG)
        list(APPEND CPACK_COMPONENTS_ALL mage_debuginfo)

        set(CPACK_DEBIAN_MAGE_DEBUGINFO_PACKAGE_NAME "memgraph-mage-debuginfo")
        set(CPACK_DEBIAN_MAGE_DEBUGINFO_FILE_NAME
            "memgraph-mage-debuginfo_${MEMGRAPH_VERSION_DEB}_${MG_ARCH_EXTENSION_DEB}.deb")
        # Unlike the mage->memgraph dep, debug symbols only make sense for
        # the exact matching build, so this one IS pinned.
        set(CPACK_DEBIAN_MAGE_DEBUGINFO_PACKAGE_DEPENDS
            "memgraph-mage (= ${MEMGRAPH_VERSION_DEB})")
        set(CPACK_DEBIAN_MAGE_DEBUGINFO_PACKAGE_SECTION "debug")
        set(CPACK_DEBIAN_MAGE_DEBUGINFO_PACKAGE_SHLIBDEPS OFF)
        set(CPACK_DEBIAN_MAGE_DEBUGINFO_DESCRIPTION
            "Debug symbols (.debug sidecars) for the matching memgraph-mage package.
 Install alongside memgraph-mage for symbolicated stack traces; gdb
 auto-locates each sidecar via .gnu_debuglink next to the corresponding
 module.")

        set(CPACK_RPM_MAGE_DEBUGINFO_PACKAGE_NAME "memgraph-mage-debuginfo")
        set(CPACK_RPM_MAGE_DEBUGINFO_FILE_NAME
            "memgraph-mage-debuginfo-${MEMGRAPH_VERSION_RPM}-1.${MG_ARCH_EXTENSION_RPM}.rpm")
        # The debuginfo spec template is generic (all @CPACK_RPM_*@ tokens),
        # so it serves both debuginfo components.
        set(CPACK_RPM_MAGE_DEBUGINFO_USER_BINARY_SPECFILE
            "${CMAKE_CURRENT_SOURCE_DIR}/rpm/memgraph-debuginfo.spec.in")
        set(CPACK_RPM_MAGE_DEBUGINFO_PACKAGE_REQUIRES
            "memgraph-mage = ${MEMGRAPH_VERSION_RPM}-1")
        set(CPACK_RPM_MAGE_DEBUGINFO_PACKAGE_AUTOREQ "no")
        set(CPACK_RPM_MAGE_DEBUGINFO_PACKAGE_SUMMARY
            "Debug symbols for memgraph-mage")
        set(CPACK_RPM_MAGE_DEBUGINFO_PACKAGE_DESCRIPTION
            "Debug symbols (.debug sidecars) for the matching memgraph-mage package.")
    endif()
endif()

# All variables must be set before including.
include(CPack)
