from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps
from conan.tools.env import VirtualBuildEnv, Environment
from conan.tools.build import check_min_cppstd, check_max_cppstd
from conan.tools.files import get, copy, replace_in_file
from conan.tools.files.symlinks import absolute_to_relative_symlinks
import os

"""
Known to work with pulsar 3.7.1, 4.0.0 -> only 4.0.0 works with boost 1.88.0, otherwise boost 1.86
and protobuf 3.21.12 and 5.29.6


"""
class PulsarClientCppConan(ConanFile):
    name = "pulsar-client-cpp"
    license = "Apache-2.0"
    url = "https://github.com/apache/pulsar-client-cpp"
    description = "Apache Pulsar C++ client library"
    topics = ("apache", "pulsar", "pub-sub", "messaging", "cpp")
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "asio": [True, False],
        "with_ssl": [True, False],
        "with_tests": [True, False],
        "with_python": [True, False],
        "with_perf_tools": [True, False],
    }
    default_options = {
        "asio": False,
        "shared": False,
        "with_ssl": True,
        "with_tests": False,
        "with_python": False,
        "with_perf_tools": False,
    }

    # Build-time tools: e.g. cmake, ninja, protoc, clang-format, doxy
    tool_requires = ["protobuf/3.21.12"]

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        self._patch_sources()

    def _patch_sources(self):
        # On Linux with Conan CMakeDeps, Protobuf is typically consumed via
        # config targets (protobuf::libprotobuf). The legacy "module" variables
        # can be empty, which leads to missing protobuf at link/compile time.
        #
        # Pulsar's legacy CMake logic also tends to fall back to the toolchain
        # Boost package, which pulls in `/opt/toolchain-v7/include` ahead of
        # Conan's protobuf headers. That produces a protoc/header version
        # mismatch, so we normalize both the Boost and Protobuf lookup paths.
        replace_in_file(
            self,
            os.path.join(self.source_folder, "LegacyFindPackages.cmake"),
            "if (APPLE AND NOT LINK_STATIC)",
            "if (NOT LINK_STATIC)",
        )
        replace_in_file(
            self,
            os.path.join(self.source_folder, "LegacyFindPackages.cmake"),
            "set(Boost_NO_BOOST_CMAKE ON)",
            "set(Boost_NO_BOOST_CMAKE OFF)",
        )
        replace_in_file(
            self,
            os.path.join(self.source_folder, "LegacyFindPackages.cmake"),
            "find_package(Boost REQUIRED)",
            "find_package(Boost REQUIRED CONFIG)",
        )
        replace_in_file(
            self,
            os.path.join(self.source_folder, "LegacyFindPackages.cmake"),
            "find_package(Boost REQUIRED COMPONENTS ${BOOST_COMPONENTS})",
            "find_package(Boost REQUIRED CONFIG COMPONENTS ${BOOST_COMPONENTS})",
        )
        replace_in_file(
            self,
            os.path.join(self.source_folder, "LegacyFindPackages.cmake"),
            "message(\"Protobuf_INCLUDE_DIRS: \" ${Protobuf_INCLUDE_DIRS})\n"
            "message(\"Protobuf_LIBRARIES: \" ${Protobuf_LIBRARIES})\n",
            "if (NOT Protobuf_INCLUDE_DIRS AND protobuf_INCLUDE_DIRS)\n"
            "    set(Protobuf_INCLUDE_DIRS ${protobuf_INCLUDE_DIRS})\n"
            "endif ()\n"
            "if (NOT Protobuf_LIBRARIES AND TARGET protobuf::libprotobuf)\n"
            "    set(Protobuf_LIBRARIES protobuf::libprotobuf)\n"
            "endif ()\n"
            "if (Protobuf_INCLUDE_DIRS)\n"
            "    include_directories(BEFORE ${Protobuf_INCLUDE_DIRS})\n"
            "endif ()\n"
            "message(\"Protobuf_INCLUDE_DIRS: \" ${Protobuf_INCLUDE_DIRS})\n"
            "message(\"Protobuf_LIBRARIES: \" ${Protobuf_LIBRARIES})\n",
        )
        replace_in_file(
            self,
            os.path.join(self.source_folder, "lib", "CMakeLists.txt"),
            "add_library(PULSAR_OBJECT_LIB OBJECT ${PULSAR_SOURCES})\n"
            "set_property(TARGET PULSAR_OBJECT_LIB PROPERTY POSITION_INDEPENDENT_CODE 1)\n"
            "if (INTEGRATE_VCPKG)\n"
            "    target_link_libraries(PULSAR_OBJECT_LIB PROTO_OBJECTS)\n"
            "endif ()\n",
            "add_library(PULSAR_OBJECT_LIB OBJECT ${PULSAR_SOURCES})\n"
            "set_property(TARGET PULSAR_OBJECT_LIB PROPERTY POSITION_INDEPENDENT_CODE 1)\n"
            "if (INTEGRATE_VCPKG)\n"
            "    target_link_libraries(PULSAR_OBJECT_LIB PROTO_OBJECTS)\n"
            "elseif (TARGET protobuf::libprotobuf)\n"
            "    target_link_libraries(PULSAR_OBJECT_LIB PRIVATE protobuf::libprotobuf)\n"
            "endif ()\n"
            "if (Protobuf_INCLUDE_DIRS)\n"
            "    target_include_directories(PULSAR_OBJECT_LIB BEFORE PRIVATE ${Protobuf_INCLUDE_DIRS})\n"
            "elseif (protobuf_INCLUDE_DIRS)\n"
            "    target_include_directories(PULSAR_OBJECT_LIB BEFORE PRIVATE ${protobuf_INCLUDE_DIRS})\n"
            "endif ()\n",
        )



    def requirements(self):
        self.requires("protobuf/3.21.12")
        self.requires("abseil/20250512.1")
        self.requires("boost/1.88.0")
        self.requires("libcurl/8.17.0")
        self.requires("zlib/1.3.1")
        self.requires("openssl/3.0.18")
        self.requires("zstd/1.5.7")
        self.requires("snappy/1.2.1")
        self.requires("gtest/1.12.1")

    def validate(self):
        check_min_cppstd(self, "11")
        check_max_cppstd(self, "26")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["CMAKE_CXX_STANDARD"] = "20"
        tc.variables["CMAKE_CXX_STANDARD_REQUIRED"] = True
        tc.variables["CMAKE_CXX_EXTENSIONS"] = False
        # Force CMake to use the Boost config generated by Conan instead of a
        # cached toolchain Boost package path.
        tc.cache_variables["Boost_DIR"] = self.generators_folder
        tc.cache_variables["BUILD_PYTHON_WRAPPER"] = bool(self.options.with_python)
        tc.cache_variables["BUILD_TESTS"] = bool(self.options.with_tests)
        tc.cache_variables["BUILD_PERF_TOOLS"] = bool(self.options.with_perf_tools)
        tc.cache_variables["BUILD_DYNAMIC_LIB"] = bool(self.options.shared)
        tc.cache_variables["BUILD_STATIC_LIB"] = not bool(self.options.shared)
        protobuf_include = None
        for dep in self.dependencies.values():
            if dep.ref and dep.ref.name == "protobuf" and dep.cpp_info.includedirs:
                protobuf_include = dep.cpp_info.includedirs[0]
                break
        # Pulsar adds -Werror globally; newer protobuf headers can trigger
        # compiler warnings that are not actionable in this recipe.
        # We also inject the Conan protobuf include dir explicitly because the
        # legacy Pulsar build can otherwise compile generated sources against
        # protobuf headers from the external toolchain.
        extra_cxx_flags = ["-Wno-error=array-bounds"]
        if protobuf_include:
            extra_cxx_flags.insert(0, f"-I{protobuf_include}")
        tc.variables["CMAKE_CXX_FLAGS"] = " ".join(extra_cxx_flags)
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()


    def build(self):
        env = Environment()
        for dep in self.dependencies.values():
             for p in dep.cpp_info.includedirs:
                 env.prepend_path("CPLUS_INCLUDE_PATH", p)
             for p in dep.cpp_info.libdirs:
                 env.prepend_path("LD_LIBRARY_PATH", p)
                 env.prepend_path("LIBRARY_PATH", p)

        # Add build output lib directory to library paths
        build_lib_dir = os.path.join(self.build_folder, "lib")
        os.makedirs(build_lib_dir, exist_ok=True)
        env.prepend_path("LIBRARY_PATH", build_lib_dir)
        env.prepend_path("LD_LIBRARY_PATH", build_lib_dir)
        with env.vars(self).apply():
            cmake = CMake(self)
            cmake.configure()
            cmake.build()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.configure()
        cmake.install()


    def package_info(self):
        self.cpp_info.libs = ["pulsar"]
