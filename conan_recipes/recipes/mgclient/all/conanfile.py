from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.scm import Version
from conan.tools.files import apply_conandata_patches, export_conandata_patches, get, copy, trim_conandata, replace_in_file
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
import os


required_conan_version = ">=1.53.0"

class MGClientConan(ConanFile):
    name = "mgclient"
    version = "1.8.0"
    description = "C/C++ Memgraph Client"
    license = "Apache-2.0"
    url = "https://github.com/memgraph/memgraph"
    homepage = "https://github.com/memgraph/mgclient"
    topics = ("memgraph", "client")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_cpp": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_cpp": True,
    }

    @property
    def _min_cppstd(self):
        return 17

    @property
    def _compilers_minimum_version(self):
        return {
            "gcc": "7",
            "clang": "7",
            "apple-clang": "10",
            "Visual Studio": "16",
            "msvc": "192",
        }

    def export(self):
        trim_conandata(self)

    def export_sources(self):
        export_conandata_patches(self)

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if not self.options.with_cpp:
            # The C library is standard-agnostic; keep libcxx/cppstd when the
            # C++ bindings are requested so validate() can enforce the minimum.
            self.settings.rm_safe("compiler.libcxx")
            self.settings.rm_safe("compiler.cppstd")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def requirements(self):
        self.requires("openssl/[>=1.1 <4]")

    def validate(self):
        if self.options.with_cpp:
            if self.settings.compiler.get_safe("cppstd"):
                check_min_cppstd(self, self._min_cppstd)
            minimum_version = self._compilers_minimum_version.get(str(self.settings.compiler), False)
            if minimum_version and Version(self.settings.compiler.version) < minimum_version:
                raise ConanInvalidConfiguration(
                    f"{self.ref} requires C++{self._min_cppstd}, which your compiler does not support."
                )

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["BUILD_CPP_BINDINGS"] = self.options.with_cpp
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def _patch_freebsd(self):
        if self.settings.os != "FreeBSD":
            return
        # SOL_TCP is Linux-specific; IPPROTO_TCP is the POSIX equivalent.
        # TCP_KEEPIDLE is Linux-specific; FreeBSD uses TCP_KEEPALIVE.
        mgsocket = os.path.join(self.source_folder, "src", "linux", "mgsocket.c")
        replace_in_file(self, mgsocket, '#include "mgsocket.h"',
                        '#include "mgsocket.h"\n\n'
                        '#ifndef SOL_TCP\n'
                        '#define SOL_TCP IPPROTO_TCP\n'
                        '#endif\n'
                        '#ifndef TCP_KEEPIDLE\n'
                        '#define TCP_KEEPIDLE TCP_KEEPALIVE\n'
                        '#endif\n')

    def build(self):
        apply_conandata_patches(self)
        self._patch_freebsd()
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, pattern="LICENSE", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.libs = ["mgclient"]

        if self.settings.os == "Windows":
            self.cpp_info.system_libs.append("ws2_32")
