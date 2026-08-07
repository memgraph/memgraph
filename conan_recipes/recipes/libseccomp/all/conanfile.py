import os

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.files import copy, get, rmdir, trim_conandata
from conan.tools.gnu import Autotools, AutotoolsToolchain
from conan.tools.layout import basic_layout


class LibseccompConan(ConanFile):
    name = "libseccomp"
    description = "Linux kernel syscall filtering (seccomp-bpf) interface library."
    url = "https://github.com/memgraph/memgraph"
    license = "LGPL-2.1-only"
    homepage = "https://github.com/seccomp/libseccomp"
    topics = ("seccomp", "syscalls", "sandboxing", "linux")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {"shared": [True, False], "fPIC": [True, False]}
    default_options = {"shared": False, "fPIC": True}

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        # Pure C library — no need for C++ stdlib settings.
        self.settings.rm_safe("compiler.libcxx")
        self.settings.rm_safe("compiler.cppstd")

    def validate(self):
        if self.settings.os != "Linux":
            raise ConanInvalidConfiguration(f"{self.ref} only supports Linux (seccomp-bpf is a Linux kernel feature).")

    def layout(self):
        basic_layout(self, src_folder="src")

    def export(self):
        # Stabilise recipe revision across conan export and local-recipes-index.
        trim_conandata(self)

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = AutotoolsToolchain(self)
        # We don't need the Python bindings — memgraph consumes the C API only.
        tc.configure_args.append("--disable-python")
        # Skip building the test/util binaries; we just want the library.
        tc.configure_args.append("--disable-gnu-symver-name")
        tc.generate()

    def build(self):
        autotools = Autotools(self)
        autotools.configure()
        autotools.make()

    def package(self):
        copy(self, "LICENSE", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        autotools = Autotools(self)
        autotools.install()
        # Strip files we don't need: man pages, pkg-config (we expose via CMakeDeps),
        # and the .la libtool archive.
        rmdir(self, os.path.join(self.package_folder, "share"))
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))
        for la in (os.path.join(self.package_folder, "lib", f) for f in os.listdir(os.path.join(self.package_folder, "lib")) if f.endswith(".la")):
            os.remove(la)

    def package_info(self):
        self.cpp_info.libs = ["seccomp"]
        # Match the upstream pkg-config name so consumers can also do
        # find_package(libseccomp) and pkg-config --libs libseccomp.
        self.cpp_info.set_property("pkg_config_name", "libseccomp")
        self.cpp_info.set_property("cmake_file_name", "libseccomp")
        self.cpp_info.set_property("cmake_target_name", "libseccomp::libseccomp")
