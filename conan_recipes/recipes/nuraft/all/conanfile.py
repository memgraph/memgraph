from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, patch, rm, rmdir
from conan.tools.scm import Version
import os

required_conan_version = ">=1.53.0"


class NuRaftConan(ConanFile):
    name = "nuraft"
    homepage = "https://github.com/eBay/NuRaft"
    description = """Cornerstone based RAFT library."""
    topics = ("raft",)
    url = "https://github.com/conan-io/conan-center-index"
    license = "Apache-2.0"

    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "asio": ["boost", "standalone"],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "asio": "standalone",
    }

    def export_sources(self):
        export_conandata_patches(self)
        copy(
            self,
            "patches/1002-standalone-asio-2.1.patch",
            src=self.recipe_folder,
            dst=self.export_sources_folder,
        )

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def build_requirements(self):
        if Version(self.version) >= 3:
            self.tool_requires("cmake/[>=3.26]")

    def requirements(self):
        if Version(self.version) < 3:
            self.requires("openssl/[>=1.1 <4]")
        if self.options.asio == "boost":
            self.requires("boost/1.81.0")
        elif Version(self.version) >= 3:
            self.requires("asio/1.36.0")

    def validate(self):
        if self.settings.os == "Windows":
            raise ConanInvalidConfiguration(f"{self.ref} doesn't support Windows")
        if Version(self.version) < 3:
            if self.settings.os == "Macos" and self.options.shared:
                raise ConanInvalidConfiguration(f"{self.ref} shared not supported for Macos")
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, 11)

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.cache_variables["DISABLE_SSL"] = 1
        if Version(self.version) >= 3:
            tc.cache_variables["WITH_CONAN"] = True
            tc.cache_variables["BOOST_ASIO"] = self.options.asio == "boost"
            tc.cache_variables["BUILD_EXAMPLES"] = False
            tc.cache_variables["BUILD_TESTING"] = False
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        apply_conandata_patches(self)
        if Version(self.version) < 3:
            if self.options.asio == "standalone":
                self.run("bash prepare.sh", cwd=self.source_folder)
                patch(
                    self,
                    base_path=self.source_folder,
                    patch_file=os.path.join(
                        self.export_sources_folder, "patches", "1002-standalone-asio-2.1.patch"
                    ),
                )
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))
        if self.options.shared:
            rm(self, "*.a", os.path.join(self.package_folder, "lib"))
            rm(self, "*.lib", os.path.join(self.package_folder, "lib"))
        else:
            rm(self, "*.so", os.path.join(self.package_folder, "lib"))
            rm(self, "*.dylib", os.path.join(self.package_folder, "lib"))
            rm(self, "*.dll", os.path.join(self.package_folder, "bin"))

    def package_info(self):
        self.cpp_info.libs = ["nuraft"]
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.extend(["m", "pthread"])
        if Version(self.version) >= 3:
            self.cpp_info.set_property("cmake_file_name", "NuRaft")
            if self.options.shared:
                self.cpp_info.set_property("cmake_target_name", "NuRaft::shared_lib")
            else:
                self.cpp_info.set_property("cmake_target_name", "NuRaft::static_lib")
