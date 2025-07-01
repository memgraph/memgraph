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
