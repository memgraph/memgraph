import os

from conan import ConanFile
from conan.errors import ConanException
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

    # TODO(matt): remove this hack so that builds end up in the subdirectory named after the build type
    # This will require fixing a bunch of the hard-coded paths in the testing code.
    def layout(self):
        # Custom layout to put everything in build/ instead of build/Release or build/RelWithDebInfo
        self.folders.build = "build"
        self.folders.generators = "build/generators"
        self.folders.package = "build/package"

    def generate(self):
        # Validate that the user toolchain file exists
        user_toolchain = self.conf.get("tools.cmake.cmaketoolchain:user_toolchain")
        if user_toolchain:
            # user_toolchain can be a list or a single path
            toolchains = user_toolchain if isinstance(user_toolchain, list) else [user_toolchain]
            for toolchain_path in toolchains:
                if not os.path.exists(toolchain_path):
                    raise ConanException(
                        f"CMake user toolchain file does not exist: {toolchain_path}\n"
                        f"This is typically configured via the profile. "
                        f"Make sure MG_TOOLCHAIN_ROOT environment variable is set correctly."
                    )

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
        self.tool_requires("ccache/4.12.1")

    def requirements(self):
        self.requires("bzip2/1.0.8")
        self.requires("fmt/11.2.0")
        # self.requires("gflags/2.2.2") # we cannot use this gflags because we have a custom one!
        self.requires("benchmark/1.9.1")
        self.requires("gtest/1.16.0")
        self.requires("spdlog/1.15.3")
        self.requires("strong_type/v15")
        self.requires("boost/1.86.0")
        self.requires("antlr4-cppruntime/4.13.2")
        self.requires("antlr4/4.13.1")
        self.requires("cppitertools/2.2")
        self.requires("ctre/3.9.0")
        self.requires("abseil/20250512.1")
        self.requires("croncpp/2023.03.30")
        self.requires("range-v3/0.12.0")
        self.requires("asio/1.34.2")
        self.requires("mgclient/1.4.3", options={"with_cpp": True})
        self.requires("snappy/1.1.9")
        self.requires("arrow/22.0.0", options={"with_s3": True, "with_snappy": True, "with_mimalloc": False})

    def package(self):
        cmake = CMake(self)
        cmake.install()
