#.rst:
# FindSeccomp
# -----------
#
# Try to locate the libseccomp library.
# If found, this will define the following variables:
#
# ``Seccomp_FOUND``
#     True if the seccomp library is available
# ``Seccomp_INCLUDE_DIRS``
#     The seccomp include directories
# ``Seccomp_LIBRARIES``
#     The seccomp libraries for linking
#
# If ``Seccomp_FOUND`` is TRUE, it will also define the following
# imported target:
#
# ``Seccomp::Seccomp``
#     The Seccomp library
#
# Since 5.44.0.

#=============================================================================
# Copyright (c) 2017 Martin Flöser <mgraesslin@kde.org>
# Copyright (c) 2017 David Kahles <david.kahles96@gmail.com>
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. The name of the author may not be used to endorse or promote products
#    derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
# OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
# IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
# NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#=============================================================================

find_package(PkgConfig QUIET)
pkg_check_modules(PKG_Libseccomp QUIET libseccomp)

find_path(Seccomp_INCLUDE_DIRS
    NAMES
        seccomp.h
    HINTS
        ${PKG_Libseccomp_INCLUDE_DIRS}
)
find_library(Seccomp_LIBRARIES
    NAMES
        seccomp
    HINTS
        ${PKG_Libseccomp_LIBRARY_DIRS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Seccomp
    FOUND_VAR
        Seccomp_FOUND
    REQUIRED_VARS
        Seccomp_LIBRARIES
        Seccomp_INCLUDE_DIRS
)

if (Seccomp_FOUND AND NOT TARGET Seccomp::Seccomp)
    add_library(Seccomp::Seccomp UNKNOWN IMPORTED)
    set_target_properties(Seccomp::Seccomp PROPERTIES
        IMPORTED_LOCATION "${Seccomp_LIBRARIES}"
        INTERFACE_INCLUDE_DIRECTORIES "${Seccomp_INCLUDE_DIRS}"
    )
endif()

mark_as_advanced(Seccomp_LIBRARIES Seccomp_INCLUDE_DIRS)

include(FeatureSummary)
set_package_properties(Seccomp PROPERTIES
    URL "https://github.com/seccomp/libseccomp"
    DESCRIPTION "The enhanced seccomp library."
)
