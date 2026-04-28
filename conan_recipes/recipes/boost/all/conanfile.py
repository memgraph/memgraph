from conan import ConanFile
from conan.errors import ConanException, ConanInvalidConfiguration
from conan.tools.apple import is_apple_os, to_apple_arch, XCRun
from conan.tools.build import build_jobs, cross_building, valid_min_cppstd, supported_cppstd
from conan.tools.env import VirtualBuildEnv
from conan.tools.files import (
    apply_conandata_patches, chdir, collect_libs, copy, export_conandata_patches,
    get, mkdir, rename, replace_in_file, rm, rmdir, save, trim_conandata
)
from conan.tools.gnu import AutotoolsToolchain
from conan.tools.layout import basic_layout
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime, MSBuildToolchain, msvc_runtime_flag, VCVars
from conan.tools.scm import Version

import glob
from io import StringIO
import os
import re
import shlex
import shutil
import sys
import yaml

required_conan_version = ">=1.53.0"

# When adding (or removing) an option, also add this option to the list in
# `rebuild-dependencies.yml` and re-run that script.
CONFIGURE_OPTIONS = (
    "atomic",
    "charconv",
    "chrono",
    "cobalt",
    "container",
    "context",
    "contract",
    "coroutine",
    "date_time",
    "exception",
    "fiber",
    "filesystem",
    "graph",
    "graph_parallel",
    "iostreams",
    "json",
    "locale",
    "log",
    "math",
    "mpi",
    "nowide",
    "process",
    "program_options",
    "python",
    "random",
    "regex",
    "serialization",
    "stacktrace",
    "system",
    "test",
    "thread",
    "timer",
    "type_erasure",
    "url",
    "wave",
)


class BoostConan(ConanFile):
    name = "boost"
    description = "Boost provides free peer-reviewed portable C++ source libraries"
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://www.boost.org"
    license = "BSL-1.0"
    topics = ("libraries", "cpp")

    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "header_only": [True, False],
        "error_code_header_only": [True, False],
        "system_no_deprecated": [True, False],
        "asio_no_deprecated": [True, False],
        "filesystem_no_deprecated": [True, False],
        "filesystem_use_std_fs": [True, False],
        "filesystem_version": [None, "3", "4"],
        "layout": ["system", "versioned", "tagged"],
        "magic_autolink": [True, False],  # enables BOOST_ALL_NO_LIB
        "diagnostic_definitions": [True, False],  # enables BOOST_LIB_DIAGNOSTIC
        "python_executable": [None, "ANY"],  # system default python installation is used, if None
        "python_version": [None, "ANY"],  # major.minor; computed automatically, if None
        "namespace": ["ANY"],  # custom boost namespace for bcp, e.g. myboost
        "namespace_alias": [True, False],  # enable namespace alias for bcp, boost=myboost
        "multithreading": [True, False],  # enables multithreading support
        "numa": [True, False],
        "zlib": [True, False],
        "bzip2": [True, False],
        "lzma": [True, False],
        "zstd": [True, False],
        "segmented_stacks": [True, False],
        "debug_level": list(range(0, 14)),
        "pch": [True, False],
        "extra_b2_flags": [None, "ANY"],  # custom b2 flags
        "i18n_backend": ["iconv", "icu", None, "deprecated"],
        "i18n_backend_iconv": ["libc", "libiconv", "off"],
        "i18n_backend_icu": [True, False],
        "visibility": ["global", "protected", "hidden"],
        "addr2line_location": ["ANY"],
        "with_stacktrace_backtrace": [True, False],
        "buildid": [None, "ANY"],
        "python_buildid": [None, "ANY"],
        "system_use_utf8": [True, False],
    }
    options.update({f"without_{_name}": [True, False] for _name in CONFIGURE_OPTIONS})

    default_options = {
        "shared": False,
        "fPIC": True,
        "header_only": False,
        "error_code_header_only": False,
        "system_no_deprecated": False,
        "asio_no_deprecated": False,
        "filesystem_no_deprecated": False,
        "filesystem_use_std_fs": False,
        "filesystem_version": None,
        "layout": "system",
        "magic_autolink": False,
        "diagnostic_definitions": False,
        "python_executable": None,
        "python_version": None,
        "namespace": "boost",
        "namespace_alias": False,
        "multithreading": True,
        "numa": True,
        "zlib": True,
        "bzip2": True,
        "lzma": False,
        "zstd": False,
        "segmented_stacks": False,
        "debug_level": 0,
        "pch": True,
        "extra_b2_flags": None,
        "i18n_backend": "deprecated",
        "i18n_backend_iconv": "libc",
        "i18n_backend_icu": False,
        "visibility": "hidden",
        "addr2line_location": "/usr/bin/addr2line",
        "with_stacktrace_backtrace": True,
        "buildid": None,
        "python_buildid": None,
        "system_use_utf8": False,
    }
    default_options.update({f"without_{_name}": False for _name in CONFIGURE_OPTIONS})
    default_options.update({f"without_{_name}": True for _name in ("graph_parallel", "mpi", "python")})

    short_paths = True
    no_copy_source = True
    _cached_dependencies = None

    def export(self):
        copy(self, f"dependencies/{self._dependency_filename}", src=self.recipe_folder, dst=self.export_folder)
        # Stabilise recipe revision across conan export and local-recipes-index
        trim_conandata(self)

    def export_sources(self):
        export_conandata_patches(self)

    def _cppstd_flag(self, compiler_cppstd=None):
        """Return the flag for the given C++ standard and compiler"""
        # TODO: Replace it by Conan tool when available: https://github.com/conan-io/conan/issues/12603
        compiler = self.settings.get_safe("compiler")
        compiler_version = self.settings.get_safe("compiler.version")
        cppstd = self.settings.get_safe("compiler.cppstd") or compiler_cppstd
        if not compiler or not compiler_version or not cppstd:
            return ""

        def _cppstd_gcc(gcc_version, cppstd):
            """Return the flag for the given C++ standard and GCC version"""
            cppstd_flags = {}
            cppstd_flags.setdefault("98", "98" if gcc_version >= "3.4" else None)
            cppstd_flags.setdefault("11", "11" if gcc_version >= "4.7" else "0x" if gcc_version >= "4.3" else None)
            cppstd_flags.setdefault("14", "14" if gcc_version >= "4.9" else "1y" if gcc_version >= "4.8" else None)
            cppstd_flags.setdefault("17", "17" if gcc_version >= "5.2" else "1z" if gcc_version >= "5" else None)
            cppstd_flags.setdefault("20", "2a" if gcc_version >= "8" else "20" if gcc_version >= "12" else None)
            cppstd_flags.setdefault("23", "2b" if gcc_version >= "11" else None)
            return cppstd_flags.get(cppstd.lstrip("gnu"))

        def _cppstd_clang(clang_version, cppstd):
            """Return the flag for the given C++ standard and Clang version"""
            cppstd_flags = {}
            cppstd_flags.setdefault("98", "98" if clang_version >= "2.1" else None)
            cppstd_flags.setdefault("11", "11" if clang_version >= "3.1" else "0x" if clang_version >= "2.1" else None)
            cppstd_flags.setdefault("14", "14" if clang_version >= "3.5" else "1y" if clang_version >= "3.4" else None)
            cppstd_flags.setdefault("17", "17" if clang_version >= "5" else "1z" if clang_version >= "3.5" else None)
            cppstd_flags.setdefault("20", "2a" if clang_version >= "6" else "20" if clang_version >= "12" else None)
            cppstd_flags.setdefault("23", "2b" if clang_version >= "13"  else "23" if clang_version >= "17" else None)
            return cppstd_flags.get(cppstd.lstrip("gnu"))


        def _cppstd_apple_clang(clang_version, cppstd):
            """Return the flag for the given C++ standard and Apple Clang version"""
            cppstd_flags = {}
            cppstd_flags.setdefault("98", "98" if clang_version >= "4.0" else None)
            cppstd_flags.setdefault("11", "11" if clang_version >= "4.0" else None)
            cppstd_flags.setdefault("14", "14" if clang_version >= "6.1" else "1y" if clang_version >= "5.1" else None)
            cppstd_flags.setdefault("17", "17" if clang_version >= "9.1" else "1z" if clang_version >= "6.1" else None)
            cppstd_flags.setdefault("20", "20" if clang_version >= "13.0" else "2a" if clang_version >= "10.0" else None)
            cppstd_flags.setdefault("23", "2b" if clang_version >= "13.0" else None)
            return cppstd_flags.get(cppstd.lstrip("gnu"))

        def _cppstd_msvc(visual_version, cppstd):
            """Return the flag for the given C++ standard and MSVC version"""
            cppstd_flags = {}
            cppstd_flags.setdefault("98", "98")
            cppstd_flags.setdefault("11", "11")
            cppstd_flags.setdefault("14", "14" if visual_version >= "190" else None)
            cppstd_flags.setdefault("17", "17" if visual_version >= "191" else "latest" if visual_version >= "190" else None)
            cppstd_flags.setdefault("20", "20" if visual_version >= "192" else "latest" if visual_version >= "191" else None)
            cppstd_flags.setdefault("23", "latest" if visual_version >= "193" else None)
            return cppstd_flags.get(cppstd)

        func = {"gcc": _cppstd_gcc, "clang": _cppstd_clang, "apple-clang": _cppstd_apple_clang, "msvc": _cppstd_msvc}.get(compiler)
        flag = cppstd
        if func:
            flag = func(Version(compiler_version), str(cppstd))
        return flag

    @property
    def _min_compiler_version_default_cxx11(self):
        """ Minimum compiler version having c++ standard >= 11
        """
        return {
            "gcc": 6,
            "clang": 6,
            "apple-clang": 99,  # still uses C++98 by default. XCode does not reflect apple-clang
            "Visual Studio": 14,  # guess
            "msvc": 190,  # guess
        }.get(str(self.settings.compiler))

    @property
    def _min_compiler_version_default_cxx14(self):
        """ Minimum compiler version having c++ standard >= 14
        https://gcc.gnu.org/gcc-6/changes.html
        https://releases.llvm.org/6.0.0/tools/clang/docs/ReleaseNotes.html#id9
        https://learn.microsoft.com/en-us/cpp/build/reference/std-specify-language-standard-version?view=msvc-150#remarks
        """
        return {
            "gcc": 6,
            "clang": 6,
            "apple-clang": 99,  # still uses C++98 by default. XCode does not reflect apple-clang
            "Visual Studio": 15,  # guess
            "msvc": 191,  # guess
        }.get(str(self.settings.compiler))

    @property
    def _min_compiler_version_default_cxx20(self):
        return {
            "gcc": 99,
            "clang": 99,
            "apple-clang": 99,
            "Visual Studio": 99,
            "msvc": 999,
        }.get(str(self.settings.compiler))

    @property
    def _has_cppstd_11_supported(self):
        cppstd = self.settings.compiler.get_safe("cppstd")
        if cppstd:
            return valid_min_cppstd(self, 11)
        compiler_version = self._min_compiler_version_default_cxx11
        if compiler_version:
            return (Version(self.settings.compiler.version) >= compiler_version) or "11" in supported_cppstd(self)

    @property
    def _has_cppstd_14_supported(self):
        cppstd = self.settings.compiler.get_safe("cppstd")
        if cppstd:
            return valid_min_cppstd(self, 14)
        required_compiler_version = self._min_compiler_version_default_cxx14
        if required_compiler_version:
            msvc_versions = {14: 190, 15: 191, 16: 192, 17: 193}
            compiler_version = Version(self.settings.compiler.version)
            is_visual_studio = str(self.settings.compiler) == "Visual Studio"
            # supported_cppstd only supports msvc, but not Visual Studio as compiler
            supported_cxx14 = "14" in supported_cppstd(self, "msvc", msvc_versions.get(compiler_version)) if is_visual_studio else "14" in supported_cppstd(self)
            # supported_cppstd: lists GCC 5 due partial support for C++14, but not enough for Boost
            return (compiler_version >= required_compiler_version) and supported_cxx14

    @property
    def _has_cppstd_20_supported(self):
        cppstd = self.settings.compiler.get_safe("cppstd")
        if cppstd:
            return valid_min_cppstd(self, 20)
        required_compiler_version = self._min_compiler_version_default_cxx20
        if required_compiler_version:
            msvc_versions = {14: 190, 15: 191, 16: 192, 17: 193}
            compiler_version = Version(self.settings.compiler.version)
            is_visual_studio = str(self.settings.compiler) == "Visual Studio"
            # supported_cppstd only supports msvc, but not Visual Studio as compiler
            supported_cxx20 = "20" in supported_cppstd(self, "msvc", msvc_versions.get(compiler_version)) if is_visual_studio else "20" in supported_cppstd(self)
            # We still dont have a compiler using C++20 by default
            return (compiler_version >= required_compiler_version) or supported_cxx20

    @property
    def _has_coroutine_supported(self):
        cppstd = self.settings.compiler.get_safe("cppstd")
        cppstd_20_supported = True
        if cppstd:
            cppstd_20_supported = valid_min_cppstd(self, 20)
        # https://en.cppreference.com/w/cpp/compiler_support#cpp20
        # https://releases.llvm.org/14.0.0/tools/clang/docs/ReleaseNotes.html#clang-format: before is experimental header
        # https://gcc.gnu.org/gcc-10/changes.html: requires -fcoroutines
        min_compiler_versions = {
                "apple-clang": "12",
                "clang": "14",
                "gcc": "10",
                "msvc": "192",
                "Visual Studio": "16",}
        required_compiler_version = min_compiler_versions.get(str(self.settings.compiler))
        if not required_compiler_version:
            return cppstd_20_supported
        return cppstd_20_supported and Version(self.settings.compiler.version) >= required_compiler_version

    @property
    def _min_compiler_version_nowide(self):
        # Nowide needs c++11 + swappable std::fstream
        return {
            "gcc": 4.8,
            "clang": 5,
            "Visual Studio": 14,  # guess
            "msvc": 190,  # guess
        }.get(str(self.settings.compiler))

    @property
    def _dependency_filename(self):
        return f"dependencies-{self.version}.yml"

    @property
    def _dependencies(self):
        if self._cached_dependencies is None:
            dependencies_filepath = os.path.join(self.recipe_folder, "dependencies", self._dependency_filename)
            if not os.path.isfile(dependencies_filepath):
                raise ConanException(f"Cannot find {dependencies_filepath}")
            with open(dependencies_filepath, encoding='utf-8') as f:
                self._cached_dependencies = yaml.safe_load(f)
        return self._cached_dependencies

    def _all_dependent_modules(self, name):
        dependencies = {name}
        while True:
            new_dependencies = set()
            for dependency in dependencies:
                new_dependencies.update(set(self._dependencies["dependencies"][dependency]))
                new_dependencies.update(dependencies)
            if len(new_dependencies) > len(dependencies):
                dependencies = new_dependencies
            else:
                break
        return dependencies

    def _all_super_modules(self, name):
        dependencies = {name}
        while True:
            new_dependencies = set(dependencies)
            for module in self._dependencies["dependencies"]:
                if dependencies.intersection(set(self._dependencies["dependencies"][module])):
                    new_dependencies.add(module)
            if len(new_dependencies) > len(dependencies):
                dependencies = new_dependencies
            else:
                break
        return dependencies

    @property
    def _bcp_dir(self):
        return "custom-boost"

    @property
    def _settings_build(self):
        return getattr(self, "settings_build", self.settings)

    @property
    def _is_clang_cl(self):
        return self.settings.os == "Windows" and self.settings.compiler == "clang"

    @property
    def _python_executable(self):
        """
        obtain full path to the python interpreter executable
        :return: path to the python interpreter executable, either set by option, or system default
        """
        exe = self.options.python_executable if self.options.python_executable else sys.executable
        return str(exe).replace("\\", "/")

    @property
    def _is_windows_platform(self):
        return self.settings.os in ["Windows", "WindowsStore", "WindowsCE"]

    @property
    def _is_apple_embedded_platform(self):
        return self.settings.os in ["iOS", "watchOS", "tvOS"]

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

        # Test whether all config_options from the yml are available in CONFIGURE_OPTIONS
        for opt_name in self._configure_options:
            if f"without_{opt_name}" not in self.options:
                raise ConanException(f"{self._dependency_filename} has the configure options {opt_name} which is not available in conanfile.py")

        # stacktrace_backtrace not supported on Windows
        if self.settings.os == "Windows":
            del self.options.with_stacktrace_backtrace

        # nowide requires a c++11-able compiler + movable std::fstream: change default to not build on compiler with too old default c++ standard or too low compiler.cppstd
        # json requires a c++11-able compiler: change default to not build on compiler with too old default c++ standard or too low compiler.cppstd
        if self.settings.compiler.get_safe("cppstd"):
            if not valid_min_cppstd(self, 11):
                self.options.without_fiber = True
                self.options.without_nowide = True
                self.options.without_json = True
                self.options.without_url = True
        else:
            version_cxx11_standard_json = self._min_compiler_version_default_cxx11
            if version_cxx11_standard_json:
                if not self._has_cppstd_11_supported:
                    self.options.without_fiber = True
                    self.options.without_json = True
                    self.options.without_nowide = True
                    self.options.without_url = True
            else:
                self.options.without_fiber = True
                self.options.without_json = True
                self.options.without_nowide = True
                self.options.without_url = True
        if Version(self.version) >= "1.85.0" and not self._has_cppstd_14_supported:
            self.options.without_math = True
        if Version(self.version) >= "1.86.0" and not self._has_cppstd_14_supported:
            self.options.without_graph = True

        # iconv is off by default on Windows and Solaris
        if self._is_windows_platform or self.settings.os == "SunOS":
            self.options.i18n_backend_iconv = "off"
        elif is_apple_os(self):
            self.options.i18n_backend_iconv = "libiconv"
        elif self.settings.os == "Android":
            # bionic provides iconv since API level 28
            api_level = self.settings.get_safe("os.api_level")
            if api_level and Version(api_level) < "28":
                self.options.i18n_backend_iconv = "libiconv"

        # Remove options not supported by this version of boost
        for dep_name in CONFIGURE_OPTIONS:
            if dep_name not in self._configure_options:
                delattr(self.options, f"without_{dep_name}")

        def disable_math():
            super_modules = self._all_super_modules("math")
            for smod in super_modules:
                try:
                    setattr(self.options, f"without_{smod}", True)
                except ConanException:
                    pass

        def disable_graph():
            super_modules = self._all_super_modules("graph")
            for smod in super_modules:
                try:
                    setattr(self.options, f"without_{smod}", True)
                except ConanException:
                    pass

        # Starting from 1.76.0, Boost.Math requires a c++11 capable compiler
        # ==> disable it by default for older compilers or c++ standards
        if self.settings.compiler.get_safe("cppstd"):
            if not valid_min_cppstd(self, 11):
                disable_math()
        else:
            min_compiler_version = self._min_compiler_version_default_cxx11
            if min_compiler_version is None:
                self.output.warning("Assuming the compiler supports c++11 by default")
            elif not self._has_cppstd_11_supported:
                disable_math()
            # Boost.Math is not built when the compiler is GCC < 5 and uses C++11
            elif self.settings.compiler == "gcc" and Version(self.settings.compiler.version) < "5":
                disable_math()

        if Version(self.version) >= "1.79.0":
            # Starting from 1.79.0, Boost.Wave requires a c++11 capable compiler
            # ==> disable it by default for older compilers or c++ standards

            def disable_wave():
                super_modules = self._all_super_modules("wave")
                for smod in super_modules:
                    try:
                        setattr(self.options, f"without_{smod}", True)
                    except ConanException:
                        pass

            if self.settings.compiler.get_safe("cppstd"):
                if not valid_min_cppstd(self, 11):
                    disable_wave()
            else:
                min_compiler_version = self._min_compiler_version_default_cxx11
                if min_compiler_version is None:
                    self.output.warning("Assuming the compiler supports c++11 by default")
                elif not self._has_cppstd_11_supported:
                    disable_wave()
                # Boost.Wave is not built when the compiler is GCC < 5 and uses C++11
                elif self.settings.compiler == "gcc" and Version(self.settings.compiler.version) < "5":
                    disable_wave()

        if Version(self.version) >= "1.81.0":
            # Starting from 1.81.0, Boost.Locale requires a c++11 capable compiler
            # ==> disable it by default for older compilers or c++ standards

            def disable_locale():
                super_modules = self._all_super_modules("locale")
                for smod in super_modules:
                    try:
                        setattr(self.options, f"without_{smod}", True)
                    except ConanException:
                        pass

            if self.settings.compiler.get_safe("cppstd"):
                if not valid_min_cppstd(self, 11):
                    disable_locale()
            else:
                min_compiler_version = self._min_compiler_version_default_cxx11
                if min_compiler_version is None:
                    self.output.warning("Assuming the compiler supports c++11 by default")
                elif not self._has_cppstd_11_supported:
                    disable_locale()
                # Boost.Locale is not built when the compiler is GCC < 5 and uses C++11
                elif self.settings.compiler == "gcc" and Version(self.settings.compiler.version) < "5":
                    disable_locale()

        if Version(self.version) >= "1.84.0":
            # Starting from 1.84.0, Boost.Cobalt requires a c++20 capable compiler
            # ==> disable it by default for older compilers or c++ standards

            def disable_cobalt():
                super_modules = self._all_super_modules("cobalt")
                for smod in super_modules:
                    try:
                        setattr(self.options, f"without_{smod}", True)
                    except ConanException:
                        pass

            if not self._has_coroutine_supported:
                disable_cobalt()
            elif self.settings.compiler.get_safe("cppstd"):
                if not valid_min_cppstd(self, 20):
                    disable_cobalt()
            else:
                min_compiler_version = self._min_compiler_version_default_cxx20
                if min_compiler_version is None:
                    self.output.warning("Assuming the compiler supports c++20 by default")
                elif Version(self.settings.compiler.version) < min_compiler_version:
                    disable_cobalt()

            # FIXME: Compilation errors on msvc shared build for boost.fiber https://github.com/boostorg/fiber/issues/314
            if is_msvc(self):
                self.options.without_fiber = True

        if Version(self.version) >= "1.85.0":
            # Starting from 1.85.0, Boost.Math requires a c++14 capable compiler
            # https://github.com/boostorg/math/blob/boost-1.85.0/README.md
            # ==> disable it by default for older compilers or c++ standards
            if self.settings.compiler.get_safe("cppstd"):
                if not valid_min_cppstd(self, 14):
                    disable_math()
            else:
                min_compiler_version = self._min_compiler_version_default_cxx14
                if min_compiler_version is None:
                    self.output.warning("Assuming the compiler supports c++14 by default")
                elif not self._has_cppstd_14_supported:
                    disable_math()

        if Version(self.version) >= "1.86.0":
            # Boost 1.86.0 updated more components that require C++14 and C++17
            # https://www.boost.org/users/history/version_1_86_0.html
            if self.settings.compiler.get_safe("cppstd"):
                if not valid_min_cppstd(self, 14):
                    disable_graph()
            else:
                min_compiler_version = self._min_compiler_version_default_cxx14
                if min_compiler_version is None:
                    self.output.warning("Assuming the compiler supports c++14 by default")
                elif not self._has_cppstd_14_supported:
                    disable_graph()

            if self.settings.os == "iOS":
                # the process library doesn't build (and doesn't even make sense) on iOS
                self.options.without_process = True

            # TODO: Revisit on Boost 1.87.0
            # It's not possible to disable process only when having shared parsed already.
            # https://github.com/boostorg/process/issues/408
            # https://github.com/boostorg/process/pull/409
            if Version(self.version) == "1.86.0" and is_msvc(self):
                setattr(self.options, "without_process", True)

        if Version(self.version) == "1.90.0":
            # FIXME: boost.coroutine doesn't support Windows ARM64 due to missing context assembly
            # See https://github.com/boostorg/context/issues/296
            if self._is_windows_platform and "arm" in str(self.settings.arch):
                self.options.without_coroutine = True

    @property
    def _configure_options(self):
        return self._dependencies["configure_options"]

    @property
    def _fPIC(self):
        return self.options.get_safe("fPIC", self.default_options["fPIC"])

    @property
    def _shared(self):
        return self.options.get_safe("shared", self.default_options["shared"])

    @property
    def _stacktrace_addr2line_available(self):
        if (self._is_apple_embedded_platform or self.settings.get_safe("os.subsystem") == "catalyst"):
             # sandboxed environment - cannot launch external processes (like addr2line), system() function is forbidden
            return False
        return not self.options.header_only and not self.options.without_stacktrace and self.settings.os != "Windows"

    @property
    def _stacktrace_from_exception_available(self):
        if Version(self.version) == "1.85.0":
            # https://github.com/boostorg/stacktrace/blob/boost-1.85.0/build/Jamfile.v2#L143
            return not self.options.header_only and not self.options.without_stacktrace and self.settings.os != "Windows"
        elif Version(self.version) >= "1.86.0":
            # https://github.com/boostorg/stacktrace/blob/boost-1.86.0/build/Jamfile.v2#L148
            return not self.options.header_only and not self.options.without_stacktrace and self._b2_architecture == "x86"

    def configure(self):
        if self.options.header_only:
            self.options.rm_safe("shared")
            self.options.rm_safe("fPIC")
        elif self.options.shared:
            self.options.rm_safe("fPIC")

        if self.options.i18n_backend != "deprecated":
            self.output.warning("i18n_backend option is deprecated, do not use anymore.")
            if self.options.i18n_backend == "iconv":
                self.options.i18n_backend_iconv = "libiconv"
                self.options.i18n_backend_icu = False
            if self.options.i18n_backend == "icu":
                self.options.i18n_backend_iconv = "off"
                self.options.i18n_backend_icu = True
            if self.options.i18n_backend == "None":
                self.options.i18n_backend_iconv = "off"
                self.options.i18n_backend_icu = False
        if self.options.without_locale:
            self.options.rm_safe("i18n_backend_iconv")
            self.options.rm_safe("i18n_backend_icu")

        if not self.options.without_python:
            if not self.options.python_version:
                self.options.python_version = self._detect_python_version()
                self.options.python_executable = self._python_executable
        else:
            self.options.rm_safe("python_buildid")

        if not self._stacktrace_addr2line_available:
            self.options.rm_safe("addr2line_location")

        if self.options.get_safe("without_stacktrace", True):
            self.options.rm_safe("with_stacktrace_backtrace")

        if self.options.without_fiber:
            self.options.rm_safe("numa")

        # Use verbosity from [conf] if specified
        verbosity = self.conf.get("tools.build:verbosity", default="quiet")
        if verbosity == "verbose" and int(self.options.debug_level) < 2:
            self.options.debug_level.value = 2

    def layout(self):
        basic_layout(self, src_folder="src")

    @property
    def _cxx11_boost_libraries(self):
        libraries = ["fiber", "json", "nowide", "url"]
        libraries.append("math")
        if Version(self.version) >= "1.79.0":
            libraries.append("wave")
        if Version(self.version) >= "1.81.0":
            libraries.append("locale")
        if Version(self.version) >= "1.84.0":
            libraries.append("atomic")
            libraries.append("filesystem")
            libraries.append("log")
            libraries.append("random")
            libraries.append("stacktrace")
            libraries.append("test")
            libraries.append("thread")
        if Version(self.version) >= "1.85.0":
            libraries.append("system")
        if Version(self.version) >= "1.89.0":
            libraries.append("atomic")
        libraries.sort()
        return list(filter(lambda library: f"without_{library}" in self.options, libraries))

    @property
    def _cxx14_boost_libraries(self):
        libraries = []
        if Version(self.version) >= "1.85.0":
            # https://github.com/boostorg/math/blob/develop/README.md#boost-math-library
            libraries.append("math")
        libraries.sort()
        return list(filter(lambda library: f"without_{library}" in self.options, libraries))

    @property
    def _cxx20_boost_libraries(self):
        libraries = []
        if Version(self.version) >= "1.84.0":
            # https://github.com/boostorg/cobalt/blob/boost-1.84.0/build/Jamfile#L54
            libraries.append("cobalt")
        libraries.sort()
        return list(filter(lambda library: f"without_{library}" in self.options, libraries))

    def validate(self):
        if not self.options.multithreading:
            # * For the reason 'thread' is deactivate look at https://stackoverflow.com/a/20991533
            #   Look also on the comments of the answer for more details
            # * Although the 'context' and 'atomic' library does not mention anything about threading,
            #   when being build the compiler uses the -pthread flag, which makes it quite dangerous
            for lib in ["locale", "coroutine", "wave", "type_erasure", "fiber", "thread", "context", "atomic"]:
                if not self.options.get_safe(f"without_{lib}"):
                    raise ConanInvalidConfiguration(f"Boost '{lib}' library requires multi threading")

        if is_msvc(self) and self._shared and is_msvc_static_runtime(self):
            raise ConanInvalidConfiguration("Boost can not be built as shared library with MT runtime.")

        # FIXME: In 1.84.0, there are compilation errors on msvc shared build for boost.fiber. https://github.com/boostorg/fiber/issues/314
        if Version(self.version) >= "1.84.0" and is_msvc(self) and self._shared and not self.options.without_fiber:
            raise ConanInvalidConfiguration("Boost.fiber can not be built as shared library on MSVC.")

        if not self.options.without_locale and self.options.i18n_backend_iconv == "off" and \
           not self.options.i18n_backend_icu and not self._is_windows_platform:
            raise ConanInvalidConfiguration(
                "Boost.Locale library needs either iconv or ICU library to be built on non windows platforms"
            )

        if self._stacktrace_addr2line_available:
            if not os.path.isabs(str(self.options.addr2line_location)):
                raise ConanInvalidConfiguration("addr2line_location must be an absolute path to addr2line")

        # Check, when a boost module is enabled, whether the boost modules it depends on are enabled as well.
        for mod_name, mod_deps in self._dependencies["dependencies"].items():
            if not self.options.get_safe(f"without_{mod_name}", True):
                for mod_dep in mod_deps:
                    if self.options.get_safe(f"without_{mod_dep}", False):
                        raise ConanInvalidConfiguration(f"{mod_name} requires {mod_deps}: {mod_dep} is disabled")

        if not self.options.get_safe("without_nowide", True):
            # nowide require a c++11-able compiler with movable std::fstream
            mincompiler_version = self._min_compiler_version_nowide
            if mincompiler_version and Version(self.settings.compiler.version) < mincompiler_version:
                raise ConanInvalidConfiguration("This compiler is too old to build Boost.nowide.")

        for cxx_standard, boost_libraries, has_cppstd_supported in [
             (11, self._cxx11_boost_libraries, self._has_cppstd_11_supported),
             (14, self._cxx14_boost_libraries, self._has_cppstd_14_supported),
             (20, self._cxx20_boost_libraries, self._has_cppstd_20_supported)]:
            if any([not self.options.get_safe(f"without_{library}", True) for library in boost_libraries]):
                if (self.settings.compiler.get_safe("cppstd") and not valid_min_cppstd(self, cxx_standard)) or \
                    not has_cppstd_supported:
                    raise ConanInvalidConfiguration(
                        f"Boost libraries {', '.join(boost_libraries)} requires a C++{cxx_standard} compiler. "
                        "Please, set compiler.cppstd or use a newer compiler version or disable from building."
                    )
        if not self.options.get_safe("without_cobalt", True) and not self._has_coroutine_supported:
            raise ConanInvalidConfiguration("Boost.Cobalt requires a C++20 capable compiler. "
                                            "Please, set compiler.cppstd and use a newer compiler version, or disable from building.")

        # TODO: Revisit on Boost 1.87.0. Remove in case Process is fixed.
        if Version(self.version) == "1.86.0" and is_msvc(self) and self.options.get_safe("shared") and self.options.get_safe("without_process", None) == False:
            raise ConanInvalidConfiguration(f"{self.ref} Boost.Process will fail to be consumed as shared library on MSVC. See https://github.com/boostorg/process/issues/408.")

    def _with_dependency(self, dependency):
        """
        Return true when dependency is required according to the dependencies-x.y.z.yml file
        """
        for name, reqs in self._dependencies["requirements"].items():
            if dependency in reqs:
                if not self.options.get_safe(f"without_{name}", True):
                    return True
        return False

    @property
    def _with_zlib(self):
        return not self.options.header_only and self._with_dependency("zlib") and self.options.zlib

    @property
    def _with_bzip2(self):
        return not self.options.header_only and self._with_dependency("bzip2") and self.options.bzip2

    @property
    def _with_lzma(self):
        return not self.options.header_only and self._with_dependency("lzma") and self.options.lzma

    @property
    def _with_zstd(self):
        return not self.options.header_only and self._with_dependency("zstd") and self.options.zstd

    @property
    def _with_icu(self):
        return not self.options.header_only and self._with_dependency("icu") and self.options.get_safe("i18n_backend_icu")

    @property
    def _with_iconv(self):
        return not self.options.header_only and self._with_dependency("iconv") and self.options.get_safe("i18n_backend_iconv") == "libiconv"

    @property
    def _with_stacktrace_backtrace(self):
        return not self.options.header_only and self.options.get_safe("with_stacktrace_backtrace", False)

    def requirements(self):
        if self._with_zlib:
            self.requires("zlib/[>=1.2.11 <2]")
        if self._with_bzip2:
            self.requires("bzip2/1.0.8")
        if self._with_lzma:
            self.requires("xz_utils/[>=5.4.5 <6]")
        if self._with_zstd:
            self.requires("zstd/[>=1.5 <1.6]")
        if self._with_stacktrace_backtrace:
            self.requires("libbacktrace/cci.20210118", transitive_headers=True, transitive_libs=True)

        if self._with_icu:
            self.requires("icu/74.2")
        if self._with_iconv:
            self.requires("libiconv/1.17")

    def package_id(self):
        del self.info.options.i18n_backend

        if self.info.options.header_only:
            self.info.clear()
        else:
            del self.info.options.debug_level
            del self.info.options.filesystem_version
            del self.info.options.pch
            del self.info.options.python_executable  # PATH to the interpreter is not important, only version matters
            if self.info.options.without_python:
                del self.info.options.python_version
            if Version(self.version) >= "1.89.0":
                del self.info.options.system_use_utf8

    def build_requirements(self):
        if not self.options.header_only:
            self.tool_requires("b2/[>=5.4 <6]")

    def source(self):
        get(self, **self.conan_data["sources"][self.version],
            destination=self.source_folder, strip_root=True)
        apply_conandata_patches(self)

    def generate(self):
        if not self.options.header_only:
            env = VirtualBuildEnv(self)
            env.generate()
            vc = VCVars(self)
            vc.generate()

    ##################### BUILDING METHODS ###########################

    def _run_python_script(self, script):
        """
        execute python one-liner script and return its output
        :param script: string containing python script to be executed
        :return: output of the python script execution, or None, if script has failed
        """
        output = StringIO()
        command = f'"{self._python_executable}" -c "{script}"'
        self.output.info(f"running {command}")
        try:
            self.run(command, output, scope="run")
        except ConanException:
            self.output.info("(failed)")
            return None
        output = output.getvalue()
        # Conan is broken when run_to_output = True
        if "\n-----------------\n" in output:
            output = output.split("\n-----------------\n", 1)[1]
        output = output.strip()
        return output if output != "None" else None

    def _get_python_path(self, name):
        """
        obtain path entry for the python installation
        :param name: name of the python config entry for path to be queried (such as "include", "platinclude", etc.)
        :return: path entry from the sysconfig
        """
        # https://docs.python.org/3/library/sysconfig.html
        # https://docs.python.org/2.7/library/sysconfig.html
        return self._run_python_script("from __future__ import print_function; "
                                       "import sysconfig; "
                                       f"print(sysconfig.get_path('{name}'))")

    def _get_python_sc_var(self, name):
        """
        obtain value of python sysconfig variable
        :param name: name of variable to be queried (such as LIBRARY or LDLIBRARY)
        :return: value of python sysconfig variable
        """
        return self._run_python_script("from __future__ import print_function; "
                                       "import sysconfig; "
                                       f"print(sysconfig.get_config_var('{name}'))")

    def _get_python_du_var(self, name):
        """
        obtain value of python distutils sysconfig variable
        (sometimes sysconfig returns empty values, while python.sysconfig provides correct values)
        :param name: name of variable to be queried (such as LIBRARY or LDLIBRARY)
        :return: value of python sysconfig variable
        """
        return self._run_python_script("from __future__ import print_function; "
                                       "import distutils.sysconfig as du_sysconfig; "
                                       f"print(du_sysconfig.get_config_var('{name}'))")

    def _get_python_var(self, name):
        """
        obtain value of python variable, either by sysconfig, or by distutils.sysconfig
        :param name: name of variable to be queried (such as LIBRARY or LDLIBRARY)
        :return: value of python sysconfig variable

        NOTE: distutils is deprecated and breaks the recipe since Python 3.10
        """
        python_version_parts = str(self.info.options.python_version).split('.')
        python_major = int(python_version_parts[0])
        python_minor = int(python_version_parts[1])
        if(python_major >= 3 and python_minor >= 10):
            return self._get_python_sc_var(name)

        return self._get_python_sc_var(name) or self._get_python_du_var(name)

    def _detect_python_version(self):
        """
        obtain version of python interpreter
        :return: python interpreter version, in format major.minor
        """
        return self._run_python_script("from __future__ import print_function; "
                                       "import sys; "
                                       "print('{}.{}'.format(sys.version_info[0], sys.version_info[1]))")

    @property
    def _python_version(self):
        version = self._detect_python_version()
        if self.options.python_version and version != self.options.python_version:
            raise ConanInvalidConfiguration(f"detected python version {version} doesn't match conan option {self.options.python_version}")
        return version

    @property
    def _python_abiflags(self):
        """
        obtain python ABI flags, see https://www.python.org/dev/peps/pep-3149/ for the details
        :return: the value of python ABI flags
        """
        return self._run_python_script("from __future__ import print_function; "
                                       "import sys; "
                                       "print(getattr(sys, 'abiflags', ''))")

    @property
    def _python_includes(self):
        """
        attempt to find directory containing Python.h header file
        :return: the directory with python includes
        """
        include = self._get_python_path("include")
        plat_include = self._get_python_path("platinclude")
        include_py = self._get_python_var("INCLUDEPY")
        include_dir = self._get_python_var("INCLUDEDIR")

        candidates = [include,
                      plat_include,
                      include_py,
                      include_dir,]
        for candidate in candidates:
            if candidate:
                python_h = os.path.join(candidate, 'Python.h')
                self.output.info(f"checking {python_h}")
                if os.path.isfile(python_h):
                    self.output.info(f"found Python.h: {python_h}")
                    return candidate.replace("\\", "/")
        raise Exception("couldn't locate Python.h - make sure you have installed python development files")

    @property
    def _python_library_dir(self):
        """
        attempt to find python development library
        :return: the full path to the python library to be linked with
        """
        library = self._get_python_var("LIBRARY")
        ldlibrary = self._get_python_var("LDLIBRARY")
        libdir = self._get_python_var("LIBDIR")
        multiarch = self._get_python_var("MULTIARCH")
        masd = self._get_python_var("multiarchsubdir")
        with_dyld = self._get_python_var("WITH_DYLD")
        if libdir and multiarch and masd and not libdir.endswith(masd):
            if masd.startswith(os.sep):
                masd = masd[len(os.sep):]
            self.output.warning(f"Python libdir candidate thingy: {libdir}")
            libdir = os.path.join(libdir, masd)

        if not libdir:
            libdest = self._get_python_var("LIBDEST")
            libdir = os.path.join(os.path.dirname(libdest), "libs")

        candidates = [ldlibrary, library]
        library_prefixes = [""] if is_msvc(self) else ["", "lib"]
        library_suffixes = [".lib"] if is_msvc(self) else [".so", ".dll.a", ".a"]
        if with_dyld:
            library_suffixes.insert(0, ".dylib")

        python_version = self._python_version
        python_version_no_dot = python_version.replace(".", "")
        versions = ["", python_version, python_version_no_dot]
        abiflags = self._python_abiflags

        for prefix in library_prefixes:
            for suffix in library_suffixes:
                for version in versions:
                    candidates.append(f"{prefix}python{version}{abiflags}{suffix}")

        for candidate in candidates:
            if candidate:
                python_lib = os.path.join(libdir, candidate)
                self.output.info(f"checking {python_lib}")
                if os.path.isfile(python_lib):
                    self.output.info(f"found python library: {python_lib}")
                    return libdir.replace("\\", "/")
        raise ConanInvalidConfiguration("couldn't locate python libraries - make sure you have installed python development files")

    def _clean(self):
        clean_dirs = [
            os.path.join(self.build_folder, "bin.v2"),
            os.path.join(self.build_folder, "architecture"),
            os.path.join(self.source_folder, self._bcp_dir),
            os.path.join(self.source_folder, "dist", "bin"),
            os.path.join(self.source_folder, "stage"),
            os.path.join(self.source_folder, "tools", "build", "src", "engine", "bootstrap"),
            os.path.join(self.source_folder, "tools", "build", "src", "engine", "bin.ntx86"),
            os.path.join(self.source_folder, "tools", "build", "src", "engine", "bin.ntx86_64"),
        ]
        for d in clean_dirs:
            if os.path.isdir(d):
                self.output.warning(f"removing '{d}'")
                shutil.rmtree(d)

    @property
    def _b2_exe(self):
        return "b2"

    @property
    def _bcp_exe(self):
        folder = os.path.join(self.source_folder, "dist", "bin")
        return os.path.join(folder, "bcp")

    @property
    def _use_bcp(self):
        return self.options.namespace != "boost"

    @property
    def _boost_build_dir(self):
        return os.path.join(self.source_folder, "tools", "build")

    def _build_bcp(self):
        folder = os.path.join(self.source_folder, "tools", "bcp")
        with chdir(self, folder):
            njobs = build_jobs(self)
            njobs = f"-j{njobs}" if njobs else ""  # boost.build doesn't take -j0 as valid
            command = f"{self._b2_exe} {njobs} --abbreviate-paths toolset={self._toolset}"
            command += f" -d{self.options.debug_level}"
            self.output.warning(command)
            self.run(command)

    def _run_bcp(self):
        with chdir(self, self.source_folder):
            mkdir(self, self._bcp_dir)
            namespace = f"--namespace={self.options.namespace}"
            alias = "--namespace-alias" if self.options.namespace_alias else ""
            boostdir = f"--boost={self.source_folder}"
            libraries = {"build", "boost-build.jam", "boostcpp.jam", "boost_install", "headers"}
            for d in os.listdir(os.path.join(self.source_folder, "boost")):
                if os.path.isdir(os.path.join(self.source_folder, "boost", d)):
                    libraries.add(d)
            for d in os.listdir(os.path.join(self.source_folder, "libs")):
                if os.path.isdir(os.path.join(self.source_folder, "libs", d)):
                    libraries.add(d)
            libraries = " ".join(libraries)
            command = f"{self._bcp_exe} {namespace} {alias} {boostdir} {libraries} {self._bcp_dir}"
            self.output.warning(command)
            self.run(command)

    def build(self):
        stacktrace_jamfile = os.path.join(self.source_folder, "libs", "stacktrace", "build", "Jamfile.v2")
        if cross_building(self, skip_x64_x86=True):
            # When cross building, do not attempt to run the test-executable (assume they work)
            replace_in_file(self, stacktrace_jamfile, "$(>) > $(<)", "echo \"\" > $(<)", strict=False)
        if self._with_stacktrace_backtrace and self.settings.os != "Windows" and not cross_building(self):
            # When libbacktrace is shared, give extra help to the test-executable
            linker_var = "DYLD_LIBRARY_PATH" if self.settings.os == "Macos" else "LD_LIBRARY_PATH"
            libbacktrace_libdir = self.dependencies["libbacktrace"].cpp_info.aggregated_components().libdirs[0]
            patched_run_rule = f"{linker_var}={libbacktrace_libdir} $(>) > $(<)"
            replace_in_file(self, stacktrace_jamfile, "$(>) > $(<)", patched_run_rule, strict=False)
            if self.dependencies["libbacktrace"].options.shared:
                replace_in_file(self, stacktrace_jamfile, "<link>static", "<link>shared", strict=False)

        # Older clang releases require a thread_local variable to be initialized by a constant value
        replace_in_file(self, os.path.join(self.source_folder, "boost", "stacktrace", "detail", "libbacktrace_impls.hpp"),
                              "/* thread_local */", "thread_local", strict=False)
        replace_in_file(self, os.path.join(self.source_folder, "boost", "stacktrace", "detail", "libbacktrace_impls.hpp"),
                              "/* static __thread */", "static __thread", strict=False)
        if self.settings.compiler == "apple-clang" or (self.settings.compiler == "clang" and Version(self.settings.compiler.version) < 6):
            replace_in_file(self, os.path.join(self.source_folder, "boost", "stacktrace", "detail", "libbacktrace_impls.hpp"),
                                  "thread_local", "/* thread_local */")
            replace_in_file(self, os.path.join(self.source_folder, "boost", "stacktrace", "detail", "libbacktrace_impls.hpp"),
                                  "static __thread", "/* static __thread */")
        replace_in_file(self, os.path.join(self.source_folder, "tools", "build", "src", "tools", "gcc.jam"),
                              "local generic-os = [ set.difference $(all-os) : aix darwin vxworks solaris osf hpux ] ;",
                              "local generic-os = [ set.difference $(all-os) : aix darwin vxworks solaris osf hpux iphone appletv ] ;",
                              strict=False)
        replace_in_file(self, os.path.join(self.source_folder, "tools", "build", "src", "tools", "gcc.jam"),
                              "local no-threading = android beos haiku sgi darwin vxworks ;",
                              "local no-threading = android beos haiku sgi darwin vxworks iphone appletv ;",
                              strict=False)
        replace_in_file(self, os.path.join(self.source_folder, "libs", "fiber", "build", "Jamfile.v2"),
                              "    <conditional>@numa",
                              "    <link>shared:<library>.//boost_fiber : <conditional>@numa",
                              strict=False)
        if self.settings.os == "Android":
            # force versionless soname from boostorg/boost#206
            # this can be applied to all versions and it's easier with a replace
            replace_in_file(self, os.path.join(self.source_folder, "boostcpp.jam"),
                            "! [ $(property-set).get <target-os> ] in windows cygwin darwin aix &&",
                            "! [ $(property-set).get <target-os> ] in windows cygwin darwin aix android &&",
                            strict=False)

        if self.options.header_only:
            self.output.warning("Header only package, skipping build")
            return

        self._clean()

        if self._use_bcp:
            self._build_bcp()
            self._run_bcp()

        self._create_user_config_jam(self._boost_build_dir)

        # JOIN ALL FLAGS
        b2_flags = " ".join(self._build_flags)
        full_command = f"{self._b2_exe} {b2_flags}"
        # -d2 is to print more debug info and avoid travis timing out without output
        sources = os.path.join(self.source_folder, self._bcp_dir) if self._use_bcp else self.source_folder
        full_command += f' --debug-configuration --build-dir="{self.build_folder}"'
        self.output.warning(full_command)

        # If sending a user-specified toolset to B2, setting the vcvars
        # interferes with the compiler selection.
        with chdir(self, sources):
            # To show the libraries *1
            # self.run("%s --show-libraries" % b2_exe)
            self.run(full_command)

    @property
    def _b2_os(self):
        return {
            "Windows": "windows",
            "WindowsStore": "windows",
            "Linux": "linux",
            "Android": "android",
            "Macos": "darwin",
            "iOS": "iphone",
            "watchOS": "iphone",
            "tvOS": "appletv",
            "FreeBSD": "freebsd",
            "SunOS": "solaris",
        }.get(str(self.settings.os))

    @property
    def _b2_address_model(self):
        if self.settings.arch in ("x86_64", "ppc64", "ppc64le", "mips64", "armv8", "armv8.3", "sparcv9", "s390x", "riscv64", "wasm64"):
            return "64"

        return "32"

    @property
    def _b2_binary_format(self):
        return {
            "Windows": "pe",
            "WindowsStore": "pe",
            "Linux": "elf",
            "Android": "elf",
            "Macos": "mach-o",
            "iOS": "mach-o",
            "watchOS": "mach-o",
            "tvOS": "mach-o",
            "FreeBSD": "elf",
            "SunOS": "elf",
        }.get(str(self.settings.os))

    @property
    def _b2_architecture(self):
        if str(self.settings.arch).startswith("x86"):
            return "x86"
        if str(self.settings.arch).startswith("ppc"):
            return "power"
        if str(self.settings.arch).startswith("arm"):
            return "arm"
        if str(self.settings.arch).startswith("sparc"):
            return "sparc"
        if str(self.settings.arch).startswith("mips64"):
            return "mips64"
        if str(self.settings.arch).startswith("mips"):
            return "mips1"
        if str(self.settings.arch).startswith("s390"):
            return "s390x"
        if str(self.settings.arch).startswith("riscv"):
            return "riscv"

        return None

    @property
    def _b2_abi(self):
        if str(self.settings.arch).startswith("x86"):
            return "ms" if str(self.settings.os) in ["Windows", "WindowsStore"] else "sysv"
        if str(self.settings.arch).startswith("ppc"):
            return "sysv"
        if str(self.settings.arch).startswith("arm"):
            return "aapcs"
        if str(self.settings.arch).startswith("mips"):
            return "o32"
        if str(self.settings.arch).startswith("riscv"):
            return "sysv"

        return None

    @property
    def _gnu_cxx11_abi(self):
        """Checks libcxx setting and returns value for the GNU C++11 ABI flag
        _GLIBCXX_USE_CXX11_ABI= .  Returns None if C++ library cannot be
        determined.
        """
        try:
            if str(self.settings.compiler.libcxx) == "libstdc++":
                return "0"
            if str(self.settings.compiler.libcxx) == "libstdc++11":
                return "1"
        except ConanException:
            pass
        return None

    @property
    def _build_flags(self):
        flags = []
        if self._build_cross_flags:
            flags.append(f'compileflags="{" ".join(self._build_cross_flags)}"')

        # Stop at the first error. No need to continue building.
        flags.append("-q")

        if self.options.get_safe("numa"):
            flags.append("numa=on")

        # https://www.boost.org/doc/libs/1_70_0/libs/context/doc/html/context/architectures.html
        if not self._is_apple_embedded_platform and self._b2_os:
            flags.append(f"target-os={self._b2_os}")
        if self._b2_architecture:
            flags.append(f"architecture={self._b2_architecture}")
        if self._b2_address_model:
            flags.append(f"address-model={self._b2_address_model}")
        if self._b2_binary_format:
            flags.append(f"binary-format={self._b2_binary_format}")
        if self._b2_abi:
            flags.append(f"abi={self._b2_abi}")

        flags.append(f"--layout={self.options.layout}")
        flags.append(f"--user-config={os.path.join(self._boost_build_dir, 'user-config.jam')}")
        flags.append(f"-sNO_ZLIB={'0' if self._with_zlib else '1'}")
        flags.append(f"-sNO_BZIP2={'0' if self._with_bzip2 else '1'}")
        flags.append(f"-sNO_LZMA={'0' if self._with_lzma else '1'}")
        flags.append(f"-sNO_ZSTD={'0' if self._with_zstd else '1'}")

        if self.options.get_safe("i18n_backend_icu"):
            flags.append("boost.locale.icu=on")
        else:
            flags.append("boost.locale.icu=off")
            flags.append("--disable-icu")
        if self.options.get_safe("i18n_backend_iconv") in ["libc", "libiconv"]:
            flags.append("boost.locale.iconv=on")
            if self.options.get_safe("i18n_backend_iconv") == "libc":
                flags.append("boost.locale.iconv.lib=libc")
            else:
                flags.append("boost.locale.iconv.lib=libiconv")
        else:
            flags.append("boost.locale.iconv=off")
            flags.append("--disable-iconv")

        def add_defines(library):
            for define in self.dependencies[library].cpp_info.aggregated_components().defines:
                flags.append(f"define={define}")

        if self._with_zlib:
            add_defines("zlib")
        if self._with_bzip2:
            add_defines("bzip2")
        if self._with_lzma:
            add_defines("xz_utils")
        if self._with_zstd:
            add_defines("zstd")

        for define in self.conf.get("tools.build:defines", default=[], check_type=list):
            flags.append(f"define={define}")

        if is_msvc(self):
            flags.append(f"runtime-link={'static' if is_msvc_static_runtime(self) else 'shared'}")
            flags.append(f"runtime-debugging={'on' if 'd' in msvc_runtime_flag(self) else 'off'}")

        # For details https://boostorg.github.io/build/manual/master/index.html
        flags.append(f"threading={'single' if not self.options.multithreading else 'multi'}")
        flags.append(f"visibility={self.options.visibility}")

        flags.append(f"link={'shared' if self._shared else 'static'}")
        if self.settings.build_type == "Debug":
            flags.append("variant=debug")
        else:
            flags.append("variant=release")

        for libname in self._configure_options:
            if not getattr(self.options, f"without_{libname}"):
                flags.append(f"--with-{libname}")

        flags.append(f"toolset={self._toolset}")

        safe_cppstd = self.settings.get_safe("compiler.cppstd")
        if safe_cppstd:
            cppstd_version = self._cppstd_flag(safe_cppstd)
            flags.append(f"cxxstd={cppstd_version}")
            if "gnu" in safe_cppstd:
                flags.append("cxxstd-dialect=gnu")
        elif Version(self.version) >= "1.85.0" and self._has_cppstd_14_supported:
            cppstd_version = self._cppstd_flag("14")
            flags.append(f"cxxstd={cppstd_version}")
        elif self._has_cppstd_11_supported:
            cppstd_version = self._cppstd_flag("11")
            flags.append(f"cxxstd={cppstd_version}")

        # LDFLAGS
        link_flags = []

        # CXX FLAGS
        cxx_flags = []
        # fPIC DEFINITION
        if self._fPIC:
            cxx_flags.append("-fPIC")
        if self.settings.build_type == "RelWithDebInfo":
            if self.settings.compiler == "gcc" or "clang" in str(self.settings.compiler):
                cxx_flags.append("-g")
            elif is_msvc(self):
                cxx_flags.append("/Z7")

        # Standalone toolchain fails when declare the std lib
        if self.settings.os not in ("Android", "Emscripten"):
            try:
                if self._gnu_cxx11_abi:
                    flags.append(f"define=_GLIBCXX_USE_CXX11_ABI={self._gnu_cxx11_abi}")

                if self.settings.compiler in ("clang", "apple-clang"):
                    libcxx = {
                        "libstdc++11": "libstdc++",
                    }.get(str(self.settings.compiler.libcxx), str(self.settings.compiler.libcxx))
                    cxx_flags.append(f"-stdlib={libcxx}")
                    link_flags.append(f"-stdlib={libcxx}")
            except ConanException:
                pass

        if self.options.error_code_header_only:
            flags.append("define=BOOST_ERROR_CODE_HEADER_ONLY=1")
        if self.options.system_no_deprecated:
            flags.append("define=BOOST_SYSTEM_NO_DEPRECATED=1")
        if self.options.asio_no_deprecated:
            flags.append("define=BOOST_ASIO_NO_DEPRECATED=1")
        if self.options.filesystem_no_deprecated:
            flags.append("define=BOOST_FILESYSTEM_NO_DEPRECATED=1")
        if self.options.filesystem_use_std_fs:
            flags.append("define=BOOST_DLL_USE_STD_FS=1")
        if self.options.system_use_utf8:
            flags.append("define=BOOST_SYSTEM_USE_UTF8=1")
        if self.options.segmented_stacks:
            flags.extend(["segmented-stacks=on",
                          "define=BOOST_USE_SEGMENTED_STACKS=1",
                          "define=BOOST_USE_UCONTEXT=1"])
        flags.append("pch=on" if self.options.pch else "pch=off")

        if is_apple_os(self):
            apple_min_version_flag = AutotoolsToolchain(self).apple_min_version_flag
            if apple_min_version_flag:
                cxx_flags.append(apple_min_version_flag)
                link_flags.append(apple_min_version_flag)
            os_subsystem = self.settings.get_safe("os.subsystem")
            if os_subsystem == "catalyst":
                cxx_flags.append("--target=arm64-apple-ios-macabi")
                link_flags.append("--target=arm64-apple-ios-macabi")

        if self.settings.os == "iOS":
            if self.options.multithreading:
                cxx_flags.append("-DBOOST_SP_USE_SPINLOCK")

            if self.conf.get("tools.apple:enable_bitcode", check_type=bool):
                cxx_flags.append("-fembed-bitcode")
        if self._with_stacktrace_backtrace:
            flags.append(f"-sLIBBACKTRACE_PATH={self.dependencies['libbacktrace'].package_folder}")
        if self._stacktrace_from_exception_available and "x86" not in str(self.settings.arch):
            # https://github.com/boostorg/stacktrace/blob/boost-1.85.0/src/from_exception.cpp#L29
            # This feature is guarded by BOOST_STACKTRACE_ALWAYS_STORE_IN_PADDING, but that is only enabled on x86.
            flags.append("define=BOOST_STACKTRACE_LIBCXX_RUNTIME_MAY_CAUSE_MEMORY_LEAK=1")
        if self._with_iconv:
            flags.append(f"-sICONV_PATH={self.dependencies['libiconv'].package_folder}")
        if self._with_icu:
            flags.append(f"-sICU_PATH={self.dependencies['icu'].package_folder}")
            if not self.dependencies["icu"].options.shared:
                # Using ICU_OPTS to pass ICU system libraries is not possible due to Boost.Regex disallowing it.
                icu_system_libs = self.dependencies["icu"].cpp_info.aggregated_components().system_libs
                if is_msvc(self):
                    icu_ldflags = " ".join(f"{l}.lib" for l in icu_system_libs)
                else:
                    icu_ldflags = " ".join(f"-l{l}" for l in icu_system_libs)
                link_flags.append(icu_ldflags)

        link_flags = f'linkflags="{" ".join(link_flags)}"'
        flags.append(link_flags)

        if self.options.get_safe("addr2line_location"):
            cxx_flags.append(f"-DBOOST_STACKTRACE_ADDR2LINE_LOCATION={self.options.addr2line_location}")

        if not self.options.get_safe('without_cobalt', True) and \
            (self.settings.compiler == "gcc" and Version(self.settings.compiler.version) == "10"):
            cxx_flags.append("-fcoroutines")

        cxx_flags = f'cxxflags="{" ".join(cxx_flags)}"'
        flags.append(cxx_flags)

        if self.options.buildid:
            flags.append(f"--buildid={self.options.buildid}")
        if not self.options.without_python and self.options.python_buildid:
            flags.append(f"--python-buildid={self.options.python_buildid}")

        if self.options.extra_b2_flags:
            flags.extend(shlex.split(str(self.options.extra_b2_flags)))

        njobs = build_jobs(self)
        njobs = f"-j{njobs}" if njobs else ""  # boost.build doesn't take -j0 as valid
        flags.extend([
            "install",
            f"--prefix={self.package_folder}",
            njobs,
            "--abbreviate-paths",
            f"-d{self.options.debug_level}",
        ])
        return flags

    @property
    def _build_cross_flags(self):
        flags = []
        if not cross_building(self):
            return flags
        arch = self.settings.get_safe("arch")
        self.output.info("Cross building, detecting compiler...")

        if arch.startswith("arm"):
            if "hf" in arch:
                flags.append("-mfloat-abi=hard")
        elif self.settings.os == "Emscripten":
            pass
        elif arch in ["x86", "x86_64"]:
            pass
        elif arch.startswith("ppc"):
            pass
        elif arch.startswith("mips"):
            pass
        elif arch.startswith("riscv"):
            pass
        else:
            self.output.warning(f"Unable to detect the appropriate ABI for {arch} architecture.")
        self.output.info(f"Cross building flags: {flags}")

        return flags

    @property
    def _ar(self):
        ar = VirtualBuildEnv(self).vars().get("AR")
        if ar:
            return ar
        if is_apple_os(self) and self.settings.compiler == "apple-clang":
            return XCRun(self).ar
        return None

    @property
    def _ranlib(self):
        ranlib = VirtualBuildEnv(self).vars().get("RANLIB")
        if ranlib:
            return ranlib
        if is_apple_os(self) and self.settings.compiler == "apple-clang":
            return XCRun(self).ranlib
        return None

    @property
    def _cxx(self):
        compilers_by_conf = self.conf.get("tools.build:compiler_executables", default={}, check_type=dict)
        cxx = compilers_by_conf.get("cpp") or VirtualBuildEnv(self).vars().get("CXX")
        if cxx:
            return cxx
        if is_apple_os(self) and self.settings.compiler == "apple-clang":
            return XCRun(self).cxx
        compiler_version = str(self.settings.compiler.version)
        major = compiler_version.split(".", maxsplit=1)[0]
        if self.settings.compiler == "gcc":
            return shutil.which(f"g++-{compiler_version}") or shutil.which(f"g++-{major}") or shutil.which("g++") or ""
        if self.settings.compiler == "clang":
            return shutil.which(f"clang++-{compiler_version}") or shutil.which(f"clang++-{major}") or shutil.which("clang++") or ""
        return ""

    def _create_user_config_jam(self, folder):
        self.output.warning("Patching user-config.jam")

        def create_library_config(deps_name, name):
            aggregated_cpp_info = self.dependencies[deps_name].cpp_info.aggregated_components()
            if len(aggregated_cpp_info.libs) == 0:
                return ""

            includedir = aggregated_cpp_info.includedirs[0].replace("\\", "/")
            includedir = f"\"{includedir}\""
            libdir = aggregated_cpp_info.libdirs[0].replace("\\", "/")
            libdir = f"\"{libdir}\""
            lib = aggregated_cpp_info.libs[0]
            version = self.dependencies[deps_name].ref.version
            return f"\nusing {name} : {version} : " \
                   f"<include>{includedir} " \
                   f"<search>{libdir} " \
                   f"<name>{lib} ;"

        contents = ""

        if self._with_zlib:
            contents += create_library_config("zlib", "zlib")
        if self._with_bzip2:
            contents += create_library_config("bzip2", "bzip2")
        if self._with_lzma:
            contents += create_library_config("xz_utils", "lzma")
        if self._with_zstd:
            contents += create_library_config("zstd", "zstd")

        if not self.options.without_python:
            # https://www.boost.org/doc/libs/1_70_0/libs/python/doc/html/building/configuring_boost_build.html
            contents += f'\nusing python : {self._python_version} : "{self._python_executable}" : "{self._python_includes}" : "{self._python_library_dir}" ;'

        if not self.options.without_mpi:
            # https://www.boost.org/doc/libs/1_72_0/doc/html/mpi/getting_started.html
            contents += "\nusing mpi ;"

        # Specify here the toolset with the binary if present if don't empty parameter :
        contents += f'\nusing "{self._toolset}" : {self._toolset_version} : '

        cxx_fwd_slahes = self._cxx.replace("\\", "/")
        if cxx_fwd_slahes:
            contents += f" \"{cxx_fwd_slahes}\""

        if is_apple_os(self):
            if self.settings.compiler == "apple-clang":
                contents += f" -isysroot {XCRun(self).sdk_path}"
            if self.settings.get_safe("arch"):
                contents += f" -arch {to_apple_arch(self)}"

        contents += " : \n"
        # Clang-specific: set <triple> to prevent b2's init-flags-cross from
        # generating --target=x86_64-pc-linux (wrong triple that breaks ASAN
        # runtime lookup). "none" disables b2's target guessing, letting clang
        # use its own default triple.
        # For cross-compilation, set tools.gnu:host_triplet in the Conan profile.
        # See https://github.com/bfgroup/b2/issues/584
        if "clang" in str(self.settings.compiler):
            triple = self.conf.get("tools.gnu:host_triplet", default="none")
            contents += f'<triple>{triple} '
        if self._ar:
            ar_path = self._ar.replace("\\", "/")
            contents += f'<archiver>"{ar_path}" '
        if self._ranlib:
            ranlib_path = self._ranlib.replace("\\", "/")
            contents += f'<ranlib>"{ranlib_path}" '
        cxxflags = " ".join(self.conf.get("tools.build:cxxflags", default=[], check_type=list)) + " "
        cflags = " ".join(self.conf.get("tools.build:cflags", default=[], check_type=list)) + " "
        buildenv_vars = VirtualBuildEnv(self).vars()
        cppflags = buildenv_vars.get("CPPFLAGS", "") + " "
        ldflags = " ".join(self.conf.get("tools.build:sharedlinkflags", default=[], check_type=list)) + " "
        asflags = buildenv_vars.get("ASFLAGS", "") + " "

        sysroot = self.conf.get("tools.build:sysroot")
        if sysroot and not is_msvc(self):
            sysroot = sysroot.replace("\\", "/")
            sysroot = f'"{sysroot}"' if ' ' in sysroot else sysroot
            cppflags += f"--sysroot={sysroot} "
            ldflags += f"--sysroot={sysroot} "

        if self._with_stacktrace_backtrace:
            backtrace_aggregated_cpp_info = self.dependencies["libbacktrace"].cpp_info.aggregated_components()
            cppflags += " ".join(f"-I{p}" for p in backtrace_aggregated_cpp_info.includedirs) + " "
            ldflags += " ".join(f"-L{p}" for p in backtrace_aggregated_cpp_info.libdirs) + " "

        if cxxflags.strip():
            contents += f'<cxxflags>"{cxxflags.strip()}" '
        if cflags.strip():
            contents += f'<cflags>"{cflags.strip()}" '
        if cppflags.strip() or self._build_cross_flags:
            compiler_flags = cppflags.strip() + " "
            compiler_flags += " ".join(self._build_cross_flags)
            contents += f'<compileflags>"{compiler_flags}" '
        if ldflags.strip():
            contents += f'<linkflags>"{ldflags.strip()}" '
        if asflags.strip():
            contents += f'<asmflags>"{asflags.strip()}" '

        if self._is_apple_embedded_platform:
            contents += f'<target-os>"{self._b2_os}" '

        contents += " ;"

        self.output.warning(contents)
        filename = f"{folder}/user-config.jam"
        save(self, filename, contents)

    @property
    def _toolset_version(self):
        toolset = MSBuildToolchain(self).toolset
        if toolset:
            match = re.match(r"v(\d+)(\d)$", toolset)
            if match:
                return f"{match.group(1)}.{match.group(2)}"
        return ""

    @property
    def _toolset(self):
        if is_msvc(self):
            return "clang-win" if self.settings.compiler.get_safe("toolset") == "ClangCL" else "msvc"
        if self.settings.os == "Windows" and self.settings.compiler == "clang":
            return "clang-win"
        if self.settings.os == "Emscripten" and self.settings.compiler in ("clang", "emcc"):
            return "emscripten"
        if self.settings.compiler == "gcc" and is_apple_os(self):
            return "darwin"
        if self.settings.compiler == "apple-clang":
            return "clang-darwin"
        if self.settings.os == "Android" and self.settings.compiler == "clang":
            return "clang-linux"
        if self.settings.compiler in ["clang", "gcc"]:
            return str(self.settings.compiler)
        if self.settings.compiler == "sun-cc":
            return "sunpro"
        if "intel" in str(self.settings.compiler):
            return {
                "Macos": "intel-darwin",
                "Windows": "intel-win",
                "Linux": "intel-linux",
            }[str(self.settings.os)]

        return str(self.settings.compiler)

    @property
    def _toolset_tag(self):
        # compiler       | compiler.version | os          | toolset_tag    | remark
        # ---------------+------------------+-------------+----------------+-----------------------------
        # apple-clang    | 12               | Macos       | darwin12       |
        # clang          | 12               | Macos       | clang-darwin12 |
        # gcc            | 11               | Linux       | gcc8           |
        # gcc            | 8                | Windows     | mgw8           |
        # Visual Studio  | 17               | Windows     | vc142          | depends on compiler.toolset
        compiler = {
            "apple-clang": "",
            "Visual Studio": "vc",
            "msvc": "vc",
        }.get(str(self.settings.compiler), str(self.settings.compiler))
        if (self.settings.compiler, self.settings.os) == ("gcc", "Windows"):
            compiler = "mgw"
        os_ = ""
        if self.settings.os == "Macos":
            os_ = "darwin"
        if is_msvc(self):
            toolset_version = self._toolset_version.replace(".", "")
        else:
            toolset_version = str(Version(self.settings.compiler.version).major)

        toolset_parts = [compiler, os_]
        toolset_tag = "-".join(part for part in toolset_parts if part) + toolset_version
        return toolset_tag

    ####################################################################

    def package(self):
        # This stage/lib is in source_folder... Face palm, looks like it builds in build but then
        # copy to source with the good lib name
        copy(self, "LICENSE_1_0.txt", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))
        if self.options.header_only:
            copy(self, "*", src=os.path.join(self.source_folder, "boost"),
                            dst=os.path.join(self.package_folder, "include", "boost"))

        if self.settings.os == "Emscripten" and not self.options.header_only:
            self._create_emscripten_libs()

        if is_msvc(self) and self._shared:
            # Some boost releases contain both static and shared variants of some libraries (if shared=True)
            all_libs = set(collect_libs(self, "lib"))
            static_libs = set(l for l in all_libs if l.startswith("lib"))
            shared_libs = all_libs.difference(static_libs)
            static_libs = set(l[3:] for l in static_libs)
            common_libs = static_libs.intersection(shared_libs)
            for common_lib in common_libs:
                common_lib_fullname = f"lib{common_lib}.lib"
                self.output.info(f'Unlinking static duplicate library: {os.path.join(self.package_folder, "lib", common_lib_fullname)}')
                os.unlink(os.path.join(self.package_folder, "lib", common_lib_fullname))

        dll_pdbs = glob.glob(os.path.join(self.package_folder, "lib", "*.dll")) + \
                    glob.glob(os.path.join(self.package_folder, "lib", "*.pdb"))
        if dll_pdbs:
            mkdir(self, os.path.join(self.package_folder, "bin"))
            for bin_file in dll_pdbs:
                rename(self, bin_file, os.path.join(self.package_folder, "bin", os.path.basename(bin_file)))

        rm(self, "*.pdb", os.path.join(self.package_folder, "bin"))
        if (is_apple_os(self) or self.settings.os == "Linux") and not self._shared and Version(self.version) >= "1.88.0":
            # FIXME: Boost 1.88 installs both .a and .dylib files for static libraries
            # https://github.com/boostorg/boost/issues/1051
            rm(self, "*.dylib", os.path.join(self.package_folder, "lib"))
            rm(self, "*.so*", os.path.join(self.package_folder, "lib"))

    def _create_emscripten_libs(self):
        # Boost Build doesn't create the libraries, but it gets close,
        # leaving .bc files where the libraries would be.
        staged_libs = os.path.join(
            self.package_folder, "lib"
        )
        if not os.path.exists(staged_libs):
            self.output.warning(f"Lib folder doesn't exist, can't collect libraries: {staged_libs}")
            return
        for bc_file in os.listdir(staged_libs):
            if bc_file.startswith("lib") and bc_file.endswith(".bc"):
                a_file = bc_file[:-3] + ".a"
                cmd = f"emar q {os.path.join(staged_libs, a_file)} {os.path.join(staged_libs, bc_file)}"
                self.output.info(cmd)
                self.run(cmd)

    @staticmethod
    def _option_to_conan_requirement(name):
        return {
            "lzma": "xz_utils",
            "iconv": "libiconv",
            "python": None,  # FIXME: change to cpython when it becomes available
        }.get(name, name)

    def package_info(self):
        self.env_info.BOOST_ROOT = self.package_folder

        self.cpp_info.set_property("cmake_file_name", "Boost")
        self.cpp_info.filenames["cmake_find_package"] = "Boost"
        self.cpp_info.filenames["cmake_find_package_multi"] = "Boost"
        self.cpp_info.names["cmake_find_package"] = "Boost"
        self.cpp_info.names["cmake_find_package_multi"] = "Boost"

        # - Use 'headers' component for all includes + defines
        # - Use '_libboost' component to attach extra system_libs, ...

        self.cpp_info.components["headers"].libs = []
        self.cpp_info.components["headers"].libdirs = []
        self.cpp_info.components["headers"].set_property("cmake_target_name", "Boost::headers")
        self.cpp_info.components["headers"].names["cmake_find_package"] = "headers"
        self.cpp_info.components["headers"].names["cmake_find_package_multi"] = "headers"
        self.cpp_info.components["headers"].names["pkg_config"] = "boost"

        if self.options.system_no_deprecated:
            self.cpp_info.components["headers"].defines.append("BOOST_SYSTEM_NO_DEPRECATED")

        if self.options.asio_no_deprecated:
            self.cpp_info.components["headers"].defines.append("BOOST_ASIO_NO_DEPRECATED")

        if self.options.filesystem_no_deprecated:
            self.cpp_info.components["headers"].defines.append("BOOST_FILESYSTEM_NO_DEPRECATED")

        if self.options.filesystem_use_std_fs:
            self.cpp_info.components["headers"].defines.append("BOOST_DLL_USE_STD_FS")

        if self.options.filesystem_version:
            self.cpp_info.components["headers"].defines.append(f"BOOST_FILESYSTEM_VERSION={self.options.filesystem_version}")

        if self.options.segmented_stacks:
            self.cpp_info.components["headers"].defines.extend(["BOOST_USE_SEGMENTED_STACKS", "BOOST_USE_UCONTEXT"])

        if self.options.system_use_utf8:
            self.cpp_info.components["headers"].defines.append("BOOST_SYSTEM_USE_UTF8")

        if self.options.buildid:
            # If you built Boost using the --buildid option then set this macro to the same value
            # as you passed to bjam.
            # For example if you built using bjam address-model=64 --buildid=amd64 then compile your code with
            # -DBOOST_LIB_BUILDID=amd64 to ensure the correct libraries are selected at link time.
            self.cpp_info.components["headers"].defines.append(f"BOOST_LIB_BUILDID={self.options.buildid}")

        if not self.options.header_only:
            if self.options.error_code_header_only:
                self.cpp_info.components["headers"].defines.append("BOOST_ERROR_CODE_HEADER_ONLY")

        if self.options.layout == "versioned":
            version = Version(self.version)
            self.cpp_info.components["headers"].includedirs.append(os.path.join("include", f"boost-{version.major}_{version.minor}"))

        # Boost::boost is an alias of Boost::headers
        self.cpp_info.components["_boost_cmake"].requires = ["headers"]
        self.cpp_info.components["_boost_cmake"].set_property("cmake_target_name", "Boost::boost")
        self.cpp_info.components["_boost_cmake"].names["cmake_find_package"] = "boost"
        self.cpp_info.components["_boost_cmake"].names["cmake_find_package_multi"] = "boost"
        if self.options.header_only:
            self.cpp_info.components["_boost_cmake"].libdirs = []

        if not self.options.header_only:
            self.cpp_info.components["_libboost"].requires = ["headers"]

            self.cpp_info.components["diagnostic_definitions"].libs = []
            self.cpp_info.components["diagnostic_definitions"].set_property("cmake_target_name", "Boost::diagnostic_definitions")
            self.cpp_info.components["diagnostic_definitions"].names["cmake_find_package"] = "diagnostic_definitions"
            self.cpp_info.components["diagnostic_definitions"].names["cmake_find_package_multi"] = "diagnostic_definitions"
            self.cpp_info.components["diagnostic_definitions"].names["pkg_config"] = "boost_diagnostic_definitions"  # FIXME: disable on pkg_config
            # I would assume headers also need the define BOOST_LIB_DIAGNOSTIC, as a header can trigger an autolink,
            # and this definition triggers a print out of the library selected.  See notes below on autolink and headers.
            self.cpp_info.components["headers"].requires.append("diagnostic_definitions")
            if self.options.diagnostic_definitions:
                self.cpp_info.components["diagnostic_definitions"].defines = ["BOOST_LIB_DIAGNOSTIC"]

            self.cpp_info.components["disable_autolinking"].libs = []
            self.cpp_info.components["disable_autolinking"].set_property("cmake_target_name", "Boost::disable_autolinking")
            self.cpp_info.components["disable_autolinking"].names["cmake_find_package"] = "disable_autolinking"
            self.cpp_info.components["disable_autolinking"].names["cmake_find_package_multi"] = "disable_autolinking"
            self.cpp_info.components["disable_autolinking"].names["pkg_config"] = "boost_disable_autolinking"  # FIXME: disable on pkg_config

            # Even headers needs to know the flags for disabling autolinking ...
            # magic_autolink is an option in the recipe, so if a consumer wants this version of boost,
            # then they should not get autolinking.
            # Note that autolinking can sneak in just by some file #including a header with (eg) boost/atomic.hpp,
            # even if it doesn't use any part that requires linking with libboost_atomic in order to compile.
            # So a boost-header-only library that links to Boost::headers needs to see BOOST_ALL_NO_LIB
            # in order to avoid autolinking to libboost_atomic

            # This define is already imported into all of the _libboost libraries from this recipe anyway,
            # so it would be better to be consistent and ensure ANYTHING using boost (headers or libs) has consistent #defines.

            # Same applies for for BOOST_AUTO_LINK_{layout}:
            # consumer libs that use headers also need to know what is the layout/filename of the libraries.
            #
            # eg, if using the "tagged" naming scheme, and a header triggers an autolink,
            # then that header's autolink request had better be configured to request the "tagged" library name.
            # Otherwise, the linker will be looking for a (eg) "versioned" library name, and there will be a link error.

            # Note that "_libboost" requires "headers" so these defines will be applied to all the libraries too.
            self.cpp_info.components["headers"].requires.append("disable_autolinking")
            if is_msvc(self) or self._is_clang_cl:
                if self.options.magic_autolink:
                    if self.options.layout == "system":
                        self.cpp_info.components["headers"].defines.append("BOOST_AUTO_LINK_SYSTEM")
                    elif self.options.layout == "tagged":
                        self.cpp_info.components["headers"].defines.append("BOOST_AUTO_LINK_TAGGED")
                    self.output.info("Enabled magic autolinking (smart and magic decisions)")
                else:
                    # DISABLES AUTO LINKING! NO SMART AND MAGIC DECISIONS THANKS!
                    self.cpp_info.components["disable_autolinking"].defines = ["BOOST_ALL_NO_LIB"]
                    self.output.info("Disabled magic autolinking (smart and magic decisions)")

            self.cpp_info.components["dynamic_linking"].libs = []
            self.cpp_info.components["dynamic_linking"].set_property("cmake_target_name", "Boost::dynamic_linking")
            self.cpp_info.components["dynamic_linking"].names["cmake_find_package"] = "dynamic_linking"
            self.cpp_info.components["dynamic_linking"].names["cmake_find_package_multi"] = "dynamic_linking"
            self.cpp_info.components["dynamic_linking"].names["pkg_config"] = "boost_dynamic_linking"  # FIXME: disable on pkg_config
            # A library that only links to Boost::headers can be linked into another library that links a Boost::library,
            # so for this reasons, the header-only library should know the BOOST_ALL_DYN_LINK definition as it will likely
            # change some important part of the boost code and cause linking errors downstream.
            # This is in the same theme as the notes above, re autolinking.
            self.cpp_info.components["headers"].requires.append("dynamic_linking")
            if self._shared:
                # A Boost::dynamic_linking cmake target does only make sense for a shared boost package
                self.cpp_info.components["dynamic_linking"].defines = ["BOOST_ALL_DYN_LINK"]

            # https://www.boost.org/doc/libs/1_73_0/more/getting_started/windows.html#library-naming
            # libsuffix for MSVC:
            # - system: ""
            # - versioned: "-vc142-mt-d-x64-1_74"
            # - tagged: "-mt-d-x64"
            libsuffix_lut = {
                "system": "",
                "versioned": "{toolset}{threading}{abi}{arch}{version}",
                "tagged": "{threading}{abi}{arch}",
            }
            libsuffix_data = {
                "toolset": f"-{self._toolset_tag}",
                "threading": "-mt" if self.options.multithreading else "",
                "abi": "",
                "ach": "",
                "version": "",
            }
            if is_msvc(self):  # FIXME: mingw?
                # FIXME: add 'y' when using cpython cci package and when python is built in debug mode
                static_runtime_key = "s" if is_msvc_static_runtime(self) else ""
                debug_runtime_key = "g" if "d" in msvc_runtime_flag(self) else ""
                debug_key = "d" if self.settings.build_type == "Debug" else ""
                abi = static_runtime_key + debug_runtime_key + debug_key
                if abi:
                    libsuffix_data["abi"] = f"-{abi}"
            else:
                debug_tag = "d" if self.settings.build_type == "Debug" else ""
                abi = debug_tag
                if abi:
                    libsuffix_data["abi"] = f"-{abi}"

            if self._b2_architecture:
                libsuffix_data["arch"] = f"-{self._b2_architecture[0]}{self._b2_address_model}"
            version = Version(self.version)
            if not version.patch or version.patch == "0":
                libsuffix_data["version"] = f"-{version.major}_{version.minor}"
            else:
                libsuffix_data["version"] = f"-{version.major}_{version.minor}_{version.patch}"
            libsuffix = libsuffix_lut[str(self.options.layout)].format(**libsuffix_data)
            if libsuffix:
                self.output.info(f"Library layout suffix: {repr(libsuffix)}")

            libformatdata = {}
            if not self.options.without_python:
                pyversion = Version(self._python_version)
                libformatdata["py_major"] = pyversion.major
                libformatdata["py_minor"] = pyversion.minor

            def add_libprefix(n):
                """ On MSVC, static libraries are built with a 'lib' prefix. Some libraries do not support shared, so are always built as a static library. """
                libprefix = ""
                if is_msvc(self) and (not self._shared or n in self._dependencies["static_only"]):
                    libprefix = "lib"
                elif self._toolset == "clang-win":
                    libprefix = "lib"
                return libprefix + n

            all_detected_libraries = set(l[:-4] if l.endswith(".dll") else l for l in collect_libs(self))
            all_expected_libraries = set()
            incomplete_components = []

            def filter_transform_module_libraries(names):
                libs = []
                for name in names:
                    if name in ("boost_stacktrace_windbg", "boost_stacktrace_windbg_cached") and self.settings.os != "Windows":
                        continue
                    if name in ("boost_math_c99l", "boost_math_tr1l") and (str(self.settings.arch).startswith("ppc") or (Version(self.version) >= "1.87.0" and self.settings.os == "Emscripten")):
                        continue
                    if name in ("boost_stacktrace_addr2line", "boost_stacktrace_backtrace", "boost_stacktrace_basic") and self.settings.os == "Windows":
                        continue
                    if name == "boost_stacktrace_from_exception" and not self._stacktrace_from_exception_available:
                        continue
                    if name == "boost_stacktrace_addr2line" and not self._stacktrace_addr2line_available:
                        continue
                    if name == "boost_stacktrace_backtrace" and self.options.get_safe("with_stacktrace_backtrace") == False:
                        continue
                    if not self.options.get_safe("numa") and "_numa" in name:
                        continue
                    new_name = add_libprefix(name.format(**libformatdata)) + libsuffix
                    if self.options.namespace != 'boost':
                        new_name = new_name.replace("boost_", str(self.options.namespace) + "_")
                    if name.startswith("boost_python") or name.startswith("boost_numpy"):
                        if self.options.python_buildid:
                            new_name += f"-{self.options.python_buildid}"
                    if self.options.buildid:
                        new_name += f"-{self.options.buildid}"
                    libs.append(new_name)
                return libs

            for module in self._dependencies["dependencies"].keys():
                missing_depmodules = list(depmodule for depmodule in self._all_dependent_modules(module) if self.options.get_safe(f"without_{depmodule}", False))
                if missing_depmodules:
                    continue

                module_libraries = filter_transform_module_libraries(self._dependencies["libs"][module])

                # Don't create components for modules that should have libraries, but don't have (because of filter)
                if self._dependencies["libs"][module] and not module_libraries:
                    continue

                all_expected_libraries = all_expected_libraries.union(module_libraries)
                if set(module_libraries).difference(all_detected_libraries):
                    incomplete_components.append(module)

                # Starting v1.69.0 Boost.System is header-only. A stub library is
                # still built for compatibility, but linking to it is no longer
                # necessary.
                # https://www.boost.org/doc/libs/1_75_0/libs/system/doc/html/system.html#changes_in_boost_1_69
                if module == "system":
                    module_libraries = []

                self.cpp_info.components[module].libs = module_libraries

                self.cpp_info.components[module].requires = self._dependencies["dependencies"][module] + ["_libboost"]
                self.cpp_info.components[module].set_property("cmake_target_name", "Boost::" + module)
                self.cpp_info.components[module].names["cmake_find_package"] = module
                self.cpp_info.components[module].names["cmake_find_package_multi"] = module
                self.cpp_info.components[module].names["pkg_config"] = f"boost_{module}"

                # extract list of names of direct host dependencies to check for dependencies
                # of components that exist in other packages
                dependencies = [d.ref.name for d, _ in self.dependencies.direct_host.items()]

                for requirement in self._dependencies.get("requirements", {}).get(module, []):
                    if self.options.get_safe(requirement, None) == False:
                        continue
                    conan_requirement = self._option_to_conan_requirement(requirement)
                    if conan_requirement not in dependencies:
                        continue
                    if module == "locale" and requirement in ("icu", "iconv"):
                        if requirement == "icu" and not self._with_icu:
                            continue
                        if requirement == "iconv" and not self._with_iconv:
                            continue
                    self.cpp_info.components[module].requires.append(f"{conan_requirement}::{conan_requirement}")

            for incomplete_component in incomplete_components:
                self.output.warning(f"Boost component '{incomplete_component}' is missing libraries. Try building boost with '-o boost:without_{incomplete_component}'. (Option is not guaranteed to exist)")

            non_used = all_detected_libraries.difference(all_expected_libraries)
            if non_used:
                raise ConanException(f"These libraries were built, but were not used in any boost module: {non_used}")

            non_built = all_expected_libraries.difference(all_detected_libraries)
            if non_built:
                raise ConanException(f"These libraries were expected to be built, but were not built: {non_built}")

            if not self.options.without_stacktrace:
                if self.settings.os in ("Linux", "FreeBSD"):
                    self.cpp_info.components["stacktrace_basic"].system_libs.append("dl")
                    if self._stacktrace_addr2line_available:
                        self.cpp_info.components["stacktrace_addr2line"].system_libs.append("dl")
                    if self._with_stacktrace_backtrace:
                        self.cpp_info.components["stacktrace_backtrace"].system_libs.append("dl")
                    if self._stacktrace_from_exception_available:
                        self.cpp_info.components["stacktrace_from_exception"].system_libs.append("dl")

                if self._stacktrace_addr2line_available:
                    self.cpp_info.components["stacktrace_addr2line"].defines.extend([
                        f"BOOST_STACKTRACE_ADDR2LINE_LOCATION=\"{self.options.addr2line_location}\"",
                        "BOOST_STACKTRACE_USE_ADDR2LINE",
                    ])

                if self._with_stacktrace_backtrace:
                    self.cpp_info.components["stacktrace_backtrace"].defines.append("BOOST_STACKTRACE_USE_BACKTRACE")
                    self.cpp_info.components["stacktrace_backtrace"].requires.append("libbacktrace::libbacktrace")

                self.cpp_info.components["stacktrace_noop"].defines.append("BOOST_STACKTRACE_USE_NOOP")

                if self.settings.os == "Windows":
                    self.cpp_info.components["stacktrace_windbg"].defines.append("BOOST_STACKTRACE_USE_WINDBG")
                    self.cpp_info.components["stacktrace_windbg"].system_libs.extend(["ole32", "dbgeng"])
                    self.cpp_info.components["stacktrace_windbg_cached"].defines.append("BOOST_STACKTRACE_USE_WINDBG_CACHED")
                    self.cpp_info.components["stacktrace_windbg_cached"].system_libs.extend(["ole32", "dbgeng"])
                elif is_apple_os(self) or self.settings.os == "FreeBSD":
                    self.cpp_info.components["stacktrace"].defines.append("BOOST_STACKTRACE_GNU_SOURCE_NOT_REQUIRED")

            if not self.options.without_python:
                pyversion = Version(self._python_version)
                python_versioned_component_name = f"python{pyversion.major}{pyversion.minor}"
                self.cpp_info.components[python_versioned_component_name].requires = ["python"]
                self.cpp_info.components[python_versioned_component_name].set_property("cmake_target_name", "Boost::" + python_versioned_component_name)
                if not self._shared:
                    self.cpp_info.components["python"].defines.append("BOOST_PYTHON_STATIC_LIB")

                numpy_versioned_component_name = f"numpy{pyversion.major}{pyversion.minor}"
                self.cpp_info.components[numpy_versioned_component_name].requires = ["numpy"]
                self.cpp_info.components[numpy_versioned_component_name].set_property("cmake_target_name", "Boost::" + numpy_versioned_component_name)

            if not self.options.get_safe("without_process"):
                if self.settings.os == "Windows":
                    self.cpp_info.components["process"].system_libs.extend(["ntdll", "shell32", "advapi32", "user32"])
                if self._shared:
                    self.cpp_info.components["process"].defines.append("BOOST_PROCESS_DYN_LINK")


            if is_msvc(self) or self._is_clang_cl:
                # https://github.com/conan-community/conan-boost/issues/127#issuecomment-404750974
                self.cpp_info.components["_libboost"].system_libs.append("bcrypt")
            elif self.settings.os == "Linux":
                # https://github.com/conan-community/community/issues/135
                self.cpp_info.components["_libboost"].system_libs.append("rt")
                if self.options.multithreading:
                    self.cpp_info.components["_libboost"].system_libs.append("pthread")
            elif self.settings.os == "Emscripten":
                if self.options.multithreading:
                    arch = str(self.settings.arch)
                    # https://emscripten.org/docs/porting/pthreads.html
                    # The documentation mentions that we should be using the "-s USE_PTHREADS=1"
                    # but it was causing problems with the target based configurations in conan
                    # So instead we are using the raw compiler flags (that are being activated
                    # from the aforementioned flag)
                    if arch.startswith("x86") or arch.startswith("wasm"):
                        self.cpp_info.components["_libboost"].cxxflags.append("-pthread")
                        self.cpp_info.components["_libboost"].sharedlinkflags.extend(["-pthread","--shared-memory"])
                        self.cpp_info.components["_libboost"].exelinkflags.extend(["-pthread","--shared-memory"])
            elif self.settings.os == "iOS":
                if self.options.multithreading:
                    # https://github.com/conan-io/conan-center-index/issues/3867
                    # runtime crashes occur when using the default platform-specific reference counter/atomic
                    # https://github.com/boostorg/filesystem/issues/147
                    # iOS should use spinlocks to avoid filesystem crashes
                    self.cpp_info.components["headers"].defines.append("BOOST_SP_USE_SPINLOCK")
                else:
                    self.cpp_info.components["headers"].defines.extend(["BOOST_AC_DISABLE_THREADS", "BOOST_SP_DISABLE_THREADS"])

            if not self.options.get_safe('without_cobalt', True) and \
                (self.settings.compiler == "gcc" and Version(self.settings.compiler.version) == "10"):
                self.cpp_info.components["cobalt"].cxxflags.append("-fcoroutines")

        #TODO: remove in the future, user_info deprecated in conan2, but kept for compatibility while recipe is cross-compatible.
        self.user_info.stacktrace_addr2line_available = self._stacktrace_addr2line_available
        self.conf_info.define("user.boost:stacktrace_addr2line_available", self._stacktrace_addr2line_available)
