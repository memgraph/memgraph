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
        # Custom layout to put everything in build/ instead of build/Release or build/RelWithDebInfo
        self.folders.build = "build"
        self.folders.generators = "build/generators"
        self.folders.package = "build/package"

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
        self.requires("zlib/1.3.1")
        self.requires("fmt/11.2.0")
        # self.requires("gflags/2.2.2") # we cannot use this gflags because we have a custom one!
        self.requires("benchmark/1.9.1")
        self.requires("gtest/1.16.0")
        self.requires("spdlog/1.15.3")
        self.requires("strong_type/v15")
        self.requires("boost/1.86.0")
        self.requires("antlr4-cppruntime/4.13.1")
        self.requires("antlr4/4.13.1")
        self.requires("cppitertools/2.2")
        self.requires("ctre/3.9.0")
        self.requires("abseil/20250127.0")
        self.requires("croncpp/2023.03.30")
        self.requires("range-v3/0.12.0")
        self.requires("asio/1.34.2")
        self.requires("openssl/3.5.1")
        self.requires("mgclient/1.4.3", options={"with_cpp": True})

    def package(self):
        cmake = CMake(self)
        cmake.install()
