import glob
import os

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, rm, rmdir, replace_in_file
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.scm import Version

required_conan_version = ">=2.0"


class RocksDBConan(ConanFile):
    name = "rocksdb"
    description = "A library that provides an embeddable, persistent key-value store for fast storage"
    license = ("GPL-2.0-only", "Apache-2.0")
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://github.com/facebook/rocksdb"
    topics = ("database", "leveldb", "facebook", "key-value")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_gflags": [True, False],
        "with_snappy": [True, False],
        "with_lz4": [True, False],
        "with_zlib": [True, False],
        "with_zstd": [True, False],
        "with_tbb": [True, False],
        "with_folly": [True, False],
        "with_jemalloc": [True, False],
        "enable_sse": [False, "sse42", "avx2"],
        "use_rtti": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_snappy": False,
        "with_lz4": False,
        "with_zlib": False,
        "with_zstd": False,
        "with_gflags": False,
        "with_tbb": False,
        "with_jemalloc": False,
        "with_folly": False,
        "enable_sse": False,
        "use_rtti": False,
    }

    def export_sources(self):
        export_conandata_patches(self)

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC
        if self.settings.arch != "x86_64":
            del self.options.with_tbb
        if self.settings.build_type == "Debug":
            self.options.use_rtti = True  # Rtti are used in asserts for debug mode...

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        if self.options.with_gflags:
            self.requires("gflags/2.2.0-memgraph")
        if self.options.with_snappy:
            self.requires("snappy/[>=1.1.10 <2]")
        if self.options.with_lz4:
            self.requires("lz4/[>=1.9.4 <2]")
        if self.options.with_zlib:
            self.requires("zlib/[>=1.2.11 <2]")
        if self.options.with_zstd:
            self.requires("zstd/[~1.5]")
        if self.options.get_safe("with_tbb"):
            self.requires("onetbb/2021.10.0")
        if self.options.with_jemalloc:
            self.requires("jemalloc/5.3.0")
        if self.options.with_folly:
            self.requires("folly/2024.08.12.00")

    def validate(self):
        check_min_cppstd(self, 17)

        if self.settings.arch not in ["x86_64", "ppc64le", "ppc64", "mips64", "armv8", "riscv64"]:
            raise ConanInvalidConfiguration("Rocksdb requires 64 bits")

        if is_msvc(self) and Version(self.settings.compiler.version) < "191":
            raise ConanInvalidConfiguration("Rocksdb requires MSVC version >= 191")

        if self.options.shared and self.options.with_folly:
            # https://github.com/facebook/rocksdb/blob/v10.5.1/CMakeLists.txt#L603
            raise ConanInvalidConfiguration(f"{self.ref} does not support a shared build with folly")

    def _patch_sources(self):
        # INFO: Avoid enforcing all linkers to use copy-dt-needed-entries
        # https://github.com/facebook/rocksdb/issues/13895
        replace_in_file(self, os.path.join(self.source_folder, "CMakeLists.txt"),
                         'set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--copy-dt-needed-entries")', "")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        apply_conandata_patches(self)
        self._patch_sources()

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["FAIL_ON_WARNINGS"] = False
        tc.variables["WITH_TESTS"] = False
        tc.variables["WITH_TOOLS"] = False
        tc.variables["WITH_CORE_TOOLS"] = False
        tc.variables["WITH_BENCHMARK_TOOLS"] = False
        tc.variables["USE_FOLLY"] = self.options.with_folly
        if is_msvc(self):
            tc.variables["WITH_MD_LIBRARY"] = not is_msvc_static_runtime(self)
        tc.variables["ROCKSDB_INSTALL_ON_WINDOWS"] = self.settings.os == "Windows"
        tc.variables["WITH_GFLAGS"] = self.options.with_gflags
        tc.variables["WITH_SNAPPY"] = self.options.with_snappy
        tc.variables["WITH_LZ4"] = self.options.with_lz4
        tc.variables["WITH_ZLIB"] = self.options.with_zlib
        tc.variables["WITH_ZSTD"] = self.options.with_zstd
        tc.variables["WITH_TBB"] = self.options.get_safe("with_tbb", False)
        tc.variables["WITH_JEMALLOC"] = self.options.with_jemalloc

        tc.variables["ROCKSDB_BUILD_SHARED"] = self.options.shared
        tc.variables["ROCKSDB_LIBRARY_EXPORTS"] = self.settings.os == "Windows" and self.options.shared
        tc.variables["ROCKSDB_DLL" ] = self.settings.os == "Windows" and self.options.shared
        tc.variables["USE_RTTI"] = self.options.use_rtti
        if not bool(self.options.enable_sse):
            tc.variables["PORTABLE"] = True
            tc.variables["FORCE_SSE42"] = False
        elif self.options.enable_sse == "sse42":
            tc.variables["PORTABLE"] = True
            tc.variables["FORCE_SSE42"] = True
        elif self.options.enable_sse == "avx2":
            tc.variables["PORTABLE"] = False
            tc.variables["FORCE_SSE42"] = False
        # not available yet in CCI
        tc.variables["WITH_NUMA"] = False
        tc.generate()

        deps = CMakeDeps(self)
        if self.options.with_jemalloc:
            deps.set_property("jemalloc", "cmake_file_name", "JeMalloc")
            deps.set_property("jemalloc", "cmake_target_name", "JeMalloc::JeMalloc")
        if self.options.with_zstd:
            deps.set_property("zstd", "cmake_target_name", "zstd::zstd")
        if self.options.with_folly:
            deps.set_property("folly", "cmake_additional_variables_prefixes", ["FOLLY",])
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def _remove_static_libraries(self):
        rm(self, "rocksdb.lib", os.path.join(self.package_folder, "lib"))
        for lib in glob.glob(os.path.join(self.package_folder, "lib", "*.a")):
            if not lib.endswith(".dll.a"):
                os.remove(lib)

    def package(self):
        copy(self, "COPYING", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        copy(self, "LICENSE*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()
        if self.options.shared:
            self._remove_static_libraries()
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))

    def package_info(self):
        cmake_target = "rocksdb-shared" if self.options.shared else "rocksdb"
        self.cpp_info.set_property("cmake_file_name", "RocksDB")
        self.cpp_info.set_property("cmake_target_name", f"RocksDB::{cmake_target}")
        # INFO: Component librocksdb is legacy due cmake_find_package but may break a few users in case removed
        lib_suffix = "-shared" if is_msvc(self) and self.options.shared else ""
        self.cpp_info.components["librocksdb"].libs = [f"rocksdb{lib_suffix}"]
        if self.settings.os == "Windows":
            self.cpp_info.components["librocksdb"].system_libs = ["shlwapi", "rpcrt4"]
            if self.options.shared:
                self.cpp_info.components["librocksdb"].defines = ["ROCKSDB_DLL"]
        elif self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.components["librocksdb"].system_libs = ["pthread", "m"]
        if self.options.with_gflags:
            self.cpp_info.components["librocksdb"].requires.append("gflags::gflags")
        if self.options.with_snappy:
            self.cpp_info.components["librocksdb"].requires.append("snappy::snappy")
        if self.options.with_lz4:
            self.cpp_info.components["librocksdb"].requires.append("lz4::lz4")
        if self.options.with_zlib:
            self.cpp_info.components["librocksdb"].requires.append("zlib::zlib")
        if self.options.with_zstd:
            self.cpp_info.components["librocksdb"].requires.append("zstd::zstd")
        if self.options.get_safe("with_tbb"):
            self.cpp_info.components["librocksdb"].requires.append("onetbb::onetbb")
        if self.options.with_jemalloc:
            self.cpp_info.components["librocksdb"].requires.append("jemalloc::jemalloc")
        if self.options.with_folly:
            self.cpp_info.components["librocksdb"].requires.append("folly::folly")
