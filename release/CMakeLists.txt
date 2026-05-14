# Install systemd service (must use absolute path).
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/memgraph.service
    DESTINATION /lib/systemd/system)

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
set(CPACK_DEBIAN_FILE_NAME "memgraph_${MEMGRAPH_VERSION_DEB}_${MG_ARCH_EXTENSION_DEB}.deb")
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/conffiles;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/copyright;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/preinst;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/prerm;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postrm;"
    "${CMAKE_CURRENT_SOURCE_DIR}/debian/postinst;")
# Auto-shlibdeps is OFF: dpkg-shlibdeps reads the build host's libc6.symbols
# (which has `Build-Depends-Package: libc6-dev` forcing minimum to whatever
# libc6 the build host ships), so building on Ubuntu 24.04 results in
# `libc6 (>= 2.38)` regardless of the symbols the binary actually uses. The
# binary itself is sysroot-clean (no symbols above GLIBC_2.30), so we write
# Depends manually with a realistic libc6 floor instead.
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)

# Description formatting is important, summary must be followed with a newline and 1 space.
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "${CPACK_PACKAGE_DESCRIPTION_SUMMARY}
 Contains Memgraph, the graph database. It aims to deliver developers the
 speed, simplicity and scale required to build the next generation of
 applications driver by real-time connected data.")

# Explicit runtime deps. Format:
#   libc6 (>= 2.31)           — matches the toolchain sysroot glibc floor.
#   libstdc++6 (>= 9)         — std::filesystem in plain libstdc++ since GCC 9
#                                (Ubuntu 20.04 LTS has libstdc++6 10).
#   libgcc-s1                 — provides libgcc_s.so.1 (Ubuntu 20.04+).
#   openssl                   — CLI; pulls libssl{3,1.1} transitively, name
#                                stable across libssl3 / libssl3t64.
#   python3 (>= 3.5.0)        — pulls libpython3 transitively. NOTE: the
#                                binary's NEEDED libpython3.X.so.1.0 must
#                                match a Python actually available on the
#                                deploy distro — set MG_PYTHON_VERSION at
#                                configure time to control this.
set(CPACK_DEBIAN_PACKAGE_DEPENDS
    "libc6 (>= 2.31), libstdc++6 (>= 9), libgcc-s1, openssl (>= 1.1.0), python3 (>= 3.5.0)")

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
set(CPACK_RPM_FILE_NAME "memgraph-${MEMGRAPH_VERSION_RPM}-1.${MG_ARCH_EXTENSION_RPM}.rpm")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    /var /var/lib /var/log /etc/logrotate.d
    /lib /lib/systemd /lib/systemd/system /lib/systemd/system/memgraph.service)
set(CPACK_RPM_PACKAGE_REQUIRES_PRE "shadow-utils")
set(CPACK_RPM_USER_BINARY_SPECFILE "${CMAKE_CURRENT_SOURCE_DIR}/rpm/memgraph.spec.in")
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
# We also depend on `python3` because we embed it in Memgraph.
set(CPACK_RPM_PACKAGE_REQUIRES "openssl >= 1.0.0, curl >= 7.29.0, python3 >= 3.5.0, logrotate")

# Configure CPack to build mgconsole using the unified install script
set(CPACK_INSTALL_SCRIPT "${CMAKE_CURRENT_SOURCE_DIR}/install_mgconsole.cmake")

# All variables must be set before including.
include(CPack)
