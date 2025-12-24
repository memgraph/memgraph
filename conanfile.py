import json
import os

from conan import ConanFile
from conan.errors import ConanException, ConanInvalidConfiguration
from conan.tools.build import can_run, check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain
from conan.tools.sbom import cyclonedx_1_6

required_conan_version = ">=2.0"


class Memgraph(ConanFile):
    name = "memgraph"
    version = "0.0.1"  # TODO
    package_type = "application"

    # license = TODO
    # author = TODO
    url = "https://github.com/memgraph/memgraph"
    homepage = "https://memgraph.com"
    description = "Conan file for Memgraph"
    settings = "os", "compiler", "build_type", "arch"

    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "src/*",
        "libs/*",
        "include/*",
        "tests/*",
        "query_modules/*",
        "release/*",
        "tools/*",
        "licenses/*",
        "config/*",
        "import/*",
    )

    default_options = {
        "aws-sdk-cpp/*:config": True,
        "aws-sdk-cpp/*:s3": True,
        "aws-sdk-cpp/*:monitoring": False,
        "aws-sdk-cpp/*:text-to-speech": False,
        "aws-sdk-cpp/*:queues": False,
        "aws-sdk-cpp/*:sqs": False,
        "aws-sdk-cpp/*:access-management": False,
        "aws-sdk-cpp/*:cognito-identity": True,
        "aws-sdk-cpp/*:iam": False,
        "aws-sdk-cpp/*:identity-management": True,
        "aws-sdk-cpp/*:transfer": True,
    }

    def requirements(self):
        # self.requires("gflags/2.2.2") # we cannot use this gflags because we have a custom one!
        self.requires("abseil/20250512.1")
        self.requires("antlr4-cppruntime/4.13.2")
        self.requires("arrow/22.0.0", options={"with_s3": True, "with_snappy": True, "with_mimalloc": False})
        self.requires(
            "aws-sdk-cpp/1.11.692",
            options={
                "config": True,
                "s3": True,
                "monitoring": False,
                "queues": False,
                "sqs": False,
                "access-management": False,
                "cognito-identity": True,
                "iam": False,
                "identity-management": True,
                "transfer": True,
                "text-to-speech": False,
            },
        )
        self.requires("asio/1.36.0")
        self.requires("boost/1.88.0")
        self.requires("bzip2/1.0.8")
        self.requires("cppitertools/2.2")
        self.requires("croncpp/2023.03.30")
        self.requires("ctre/3.10.0")
        self.requires("fmt/11.2.0")
        self.requires("libcurl/8.17.0", override=True)
        self.requires("mgclient/1.4.3", options={"with_cpp": True})
        self.requires("protobuf/3.20.3")
        self.requires("range-v3/0.12.0")
        self.requires("simdjson/4.2.2")
        self.requires("snappy/1.2.1", override=True)
        self.requires("spdlog/1.15.3")
        self.requires("strong_type/v15")
        self.requires("zlib/1.3.1")

    def build_requirements(self):
        self.tool_requires("cmake/4.1.2")
        self.tool_requires("ninja/1.13.1")
        self.tool_requires("ccache/4.12.1")
        self.tool_requires("antlr4/4.13.1")

        self.test_requires("benchmark/1.9.4")
        self.test_requires("gtest/1.17.0")

    def validate(self):
        """Validate configuration before generation"""
        # Memgraph only supports Linux
        if self.settings.os != "Linux":
            raise ConanInvalidConfiguration("Memgraph only supports Linux")

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

    def validate_build(self):
        """Validate build configuration"""
        # Memgraph requires C++20 or higher
        check_min_cppstd(self, "20")

    def package_id(self):
        # Remove compiler version from package_id for applications
        # Keep build_type to maintain separate Debug/Release binaries
        del self.info.settings.compiler

    # TODO(matt): remove this hack so that builds end up in the subdirectory named after the build type
    # This will require fixing a bunch of the hard-coded paths in the testing code.
    def layout(self):
        # Custom layout to put everything in build/ instead of build/Release or build/RelWithDebInfo
        self.folders.build = "build"
        self.folders.generators = "build/generators"
        self.folders.package = "build/package"

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)

        # Automatically set sanitizer CMake flags based on compiler settings
        if self.settings.get_safe("compiler.asan"):
            tc.cache_variables["ASAN"] = "ON"

        if self.settings.get_safe("compiler.ubsan"):
            tc.cache_variables["UBSAN"] = "ON"

        if self.settings.get_safe("compiler.tsan"):
            tc.cache_variables["TSAN"] = "ON"

        tc.generate()

        # SBOM generation
        sbom = cyclonedx_1_6(self, name="memgraph", add_build=False, add_tests=False)
        out_dir = os.path.join(self.generators_folder, "sbom")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, "memgraph-sbom.cdx.json")
        with open(out_path, "w") as f:
            json.dump(sbom, f, indent=2)

        self.output.success(f"CYCLONEDX SBOM CREATED -> {out_path}")

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        # Only run unit tests if we can run them (not cross-compiling)
        if can_run(self):
            cmake.ctest(cli_args=["-R", "memgraph__unit", "--output-on-failure"])

    def package(self):
        # NOTE we still can't package because ATM we exports_sources which means:
        # - we are not a git repo
        # - releases/get_version.py will fail
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        # Memgraph is an application, so we declare the bin directory
        # This allows tool_requires consumers to find memgraph in PATH
        self.cpp_info.bindirs = ["bin"]
