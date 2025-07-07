from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout


class Memgraph(ConanFile):
    name = "memgraph"
    version = "0.0.1"
    package_type = "application"

    # license = TODO
    # author = TODO
    description = "Conan file for Memgraph"
    settings = "os", "compiler", "build_type", "arch"

    exports_sources = "CMakeLists.txt", "src/*"

    def layout(self):
        cmake_layout(self)

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def build_requirements(self):
        self.tool_requires("cmake/4.0.3")
        self.tool_requires("ninja/1.11.1")

    def requirements(self):
        self.requires("bzip2/1.0.8")  # MG currently uses 1.0.6
        self.requires("zlib/1.2.11")
        self.requires("fmt/8.0.1")
        # self.requires("boost/1.78.0")
        # self.requires("antlr4/4.13.1")
        # self.requires("antlr4-cppruntime/4.13.2")

    def package(self):
        cmake = CMake(self)
        cmake.install()
