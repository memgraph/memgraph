from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMake, CMakeToolchain, CMakeDeps
from conan.errors import ConanInvalidConfiguration
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, trim_conandata
from conan.tools.build import check_min_cppstd
from conan.tools.scm import Version
from conan.tools.microsoft import check_min_vs, is_msvc
import os

required_conan_version = ">=2.0.9"


class CcacheConan(ConanFile):
    name = "ccache"
    package_type = "application"
    description = (
        "Ccache (or “ccache”) is a compiler cache. It speeds up recompilation "
        "by caching previous compilations and detecting when the same "
        "compilation is being done again."
    )
    license = "GPL-3.0-or-later"
    topics = ("compiler-cache", "recompilation", "cache", "compiler")
    homepage = "https://ccache.dev"
    url = "https://github.com/conan-io/conan-center-index"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "redis_storage_backend": [True, False],
    }
    default_options = {
        "redis_storage_backend": True,
    }

    @property
    def _min_cppstd(self):
        return "17"

    @property
    def _compilers_minimum_version(self):
        return {
            "gcc": "8",
            "clang": "9",
            "apple-clang": "11",
        }

    def export(self):
        trim_conandata(self)

    def export_sources(self):
        export_conandata_patches(self)

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        self.requires("zstd/[>=1.5 <1.6]")
        if self.options.redis_storage_backend:
            if Version(self.version) >= "4.12":
                self.requires("hiredis/1.3.0")
            else:
                self.requires("hiredis/1.2.0")

        if Version(self.version) >= "4.10":
            self.requires("fmt/[>=10.2.1 <=11.1.1]") # Explicitly tested with all versions in this range
            self.requires("xxhash/[~0.8]")


    def validate(self):
        check_min_cppstd(self, self._min_cppstd)
        check_min_vs(self, 192)
        if not is_msvc(self):
            minimum_version = self._compilers_minimum_version.get(str(self.settings.compiler), False)
            if minimum_version and Version(self.settings.compiler.version) < minimum_version:
                raise ConanInvalidConfiguration(
                    f"{self.ref} requires C++{self._min_cppstd}, which your compiler does not support."
                )
        if self.settings.compiler== "clang" and Version(self.settings.compiler.version).major == "11" and \
            self.settings.compiler.libcxx == "libstdc++":
            raise ConanInvalidConfiguration(f"{self.ref} requires C++ filesystem library, that is not supported by Clang 11 + libstdc++.")

        if self.settings.os == "Windows" and self.settings.arch == "armv8" and Version(self.version) < "4.10":
            raise ConanInvalidConfiguration("ccache does not support ARMv8 on Windows before version 4.10")

    def build_requirements(self):
        self.tool_requires("cmake/[>=3.18]")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], destination=self.source_folder,
            strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["REDIS_STORAGE_BACKEND"] = self.options.redis_storage_backend
        tc.variables["HIREDIS_FROM_INTERNET"] = False
        tc.variables["ZSTD_FROM_INTERNET"] = False
        tc.variables["ENABLE_DOCUMENTATION"] = False
        tc.variables["ENABLE_TESTING"] = False
        tc.variables["STATIC_LINK"] = False  # Don't link static runtimes and let Conan handle it
        tc.generate()

        deps = CMakeDeps(self)
        if Version(self.version) >= "4.10":
            deps.set_property("fmt", "cmake_file_name", "Fmt")
            deps.set_property("fmt", "cmake_find_mode", "module")
            deps.set_property("fmt", "cmake_target_name", "dep_fmt")
            deps.set_property("zstd", "cmake_file_name", "Zstd")
            deps.set_property("zstd", "cmake_target_name", "dep_zstd")
            deps.set_property("hiredis", "cmake_file_name", "Hiredis")
            deps.set_property("hiredis", "cmake_target_name", "dep_hiredis")
        else:
            deps.set_property("hiredis", "cmake_target_name", "HIREDIS::HIREDIS")
            deps.set_property("zstd", "cmake_target_name", "ZSTD::ZSTD")
        deps.set_property("zstd", "cmake_find_mode", "module")
        deps.set_property("hiredis", "cmake_find_mode", "module")
        deps.generate()

    def build(self):
        apply_conandata_patches(self)
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "*GPL-*.txt", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libdirs = []
        self.cpp_info.includedirs = []
