from conan import ConanFile
from conan.errors import ConanException
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import collect_libs, copy, get, rmdir
from conan.tools.scm import Version
import os

required_conan_version = ">=2.1"


class GflagsConan(ConanFile):
    name = "gflags"
    description = "The gflags package contains a C++ library that implements commandline flags processing"
    topics = ("cli", "flags", "commandline")
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://github.com/gflags/gflags"
    license = "BSD-3-Clause"

    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "nothreads": [True, False],
        "namespace": ["ANY"],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "nothreads": True,
        "namespace": "gflags",
    }

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["BUILD_SHARED_LIBS"] = self.options.shared
        tc.variables["BUILD_STATIC_LIBS"] = not self.options.shared
        tc.variables["BUILD_gflags_LIB"] = not self.options.nothreads
        tc.variables["BUILD_gflags_nothreads_LIB"] = self.options.nothreads
        tc.variables["BUILD_PACKAGING"] = False
        tc.variables["BUILD_TESTING"] = False
        tc.variables["INSTALL_HEADERS"] = True
        tc.variables["INSTALL_SHARED_LIBS"] = self.options.shared
        tc.variables["INSTALL_STATIC_LIBS"] = not self.options.shared
        tc.variables["REGISTER_BUILD_DIR"] = False
        tc.variables["REGISTER_INSTALL_PREFIX"] = False
        tc.variables["GFLAGS_NAMESPACE"] = self.options.namespace
        tc.cache_variables["CMAKE_POLICY_VERSION_MINIMUM"] = "3.5" # CMake 4 support
        if Version(self.version) > "2.2.2": # pylint: disable=conan-unreachable-upper-version
            raise ConanException("CMAKE_POLICY_VERSION_MINIMUM hardcoded to 3.5, check if new version supports CMake 4")
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "COPYING.txt", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "gflags")
        self.cpp_info.set_property("cmake_target_name", "gflags::gflags")
        self.cpp_info.set_property("cmake_target_aliases", ["gflags"])
        self.cpp_info.set_property("pkg_config_name", "gflags")
        self.cpp_info.libs = collect_libs(self)
        if self.settings.os == "Windows":
            self.cpp_info.system_libs.extend(["shlwapi"])
        elif self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.extend(["pthread", "m"])
