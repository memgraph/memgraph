import os

from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, trim_conandata
from conan.tools.layout import basic_layout


class NlohmannJsonConan(ConanFile):
    name = "nlohmann_json"
    version = "3.11.3-memgraph"
    homepage = "https://github.com/nlohmann/json"
    description = "JSON for Modern C++ parser and generator (Memgraph patched)."
    topics = "json", "header-only"
    url = "https://github.com/memgraph/memgraph"
    license = "MIT"
    package_type = "header-library"
    settings = "os", "arch", "compiler", "build_type"
    no_copy_source = True

    def export(self):
        trim_conandata(self)

    def export_sources(self):
        export_conandata_patches(self)

    def layout(self):
        basic_layout(self, src_folder="src")

    def package_id(self):
        self.info.clear()

    def validate(self):
        if self.settings.compiler.cppstd:
            check_min_cppstd(self, 11)

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        apply_conandata_patches(self)

    def generate(self):
        pass

    def build(self):
        pass

    def package(self):
        copy(self, "LICENSE*", self.source_folder, os.path.join(self.package_folder, "licenses"))
        copy(self, "*", os.path.join(self.source_folder, "include"), os.path.join(self.package_folder, "include"))

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "nlohmann_json")
        self.cpp_info.set_property("cmake_target_name", "nlohmann_json::nlohmann_json")
        self.cpp_info.set_property("pkg_config_name", "nlohmann_json")
        self.cpp_info.bindirs = []
        self.cpp_info.libdirs = []
