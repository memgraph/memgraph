from conan import ConanFile
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, trim_conandata
from conan.tools.layout import basic_layout
import os

required_conan_version = ">=2.0"


class USearchConan(ConanFile):
    name = "usearch"
    description = "USearch packaged with Conan dependencies for selected vendored headers"
    license = "Apache-2.0"
    topics = ("search", "vector", "simd", "hnsw")
    homepage = "https://github.com/unum-cloud/usearch"
    url = "https://github.com/unum-cloud/usearch"
    package_type = "header-library"
    settings = "os", "arch", "compiler", "build_type"
    no_copy_source = True

    def export(self):
        # Stabilise recipe revision across conan export and local-recipes-index
        trim_conandata(self)

    def export_sources(self):
        export_conandata_patches(self)

    def layout(self):
        basic_layout(self, src_folder="src")

    def package_id(self):
        self.info.clear()

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        apply_conandata_patches(self)

    def package(self):
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        copy(
            self,
            pattern="*.h",
            src=os.path.join(self.source_folder, "include"),
            dst=os.path.join(self.package_folder, "include"),
        )
        copy(
            self,
            pattern="*.hpp",
            src=os.path.join(self.source_folder, "include"),
            dst=os.path.join(self.package_folder, "include"),
        )

    def package_info(self):
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
        self.cpp_info.set_property("cmake_file_name", "usearch")
        self.cpp_info.set_property("cmake_target_name", "usearch::usearch")
        self.cpp_info.set_property("pkg_config_name", "usearch")
        # With the SIMD kernels off, the headers include nothing outside the standard
        # library, so this package carries no dependencies of its own.
        self.cpp_info.defines.extend([
            "USEARCH_USE_OPENMP=0",
            "USEARCH_USE_NUMKONG=0",
        ])
