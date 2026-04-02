import os
import shutil

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.env import VirtualBuildEnv
from conan.tools.files import apply_conandata_patches, copy, export_conandata_patches, get, rename, rmdir
from conan.tools.gnu import Autotools, AutotoolsToolchain
from conan.tools.layout import basic_layout
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.scm import Version


class JemallocConan(ConanFile):
    name = "jemalloc"
    version = "5.2.1-memgraph"
    description = "jemalloc general purpose malloc(3) (Memgraph configuration)."
    url = "https://github.com/memgraph/memgraph"
    license = "BSD-2-Clause"
    homepage = "https://jemalloc.net/"
    topics = ("jemalloc", "malloc", "free")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "prefix": ["ANY"],
        "enable_cxx": [True, False],
        "enable_fill": [True, False],
        "enable_xmalloc": [True, False],
        "enable_readlinkat": [True, False],
        "enable_syscall": [True, False],
        "enable_lazy_lock": [True, False],
        "enable_debug_logging": [True, False],
        "enable_initial_exec_tls": [True, False],
        "enable_libdl": [True, False],
        "enable_prof": [True, False],
        "lg_page": ["ANY"],
        "lg_hugepage": ["ANY"],
        "malloc_conf": ["ANY"],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "prefix": "",
        "enable_cxx": True,
        "enable_fill": True,
        "enable_xmalloc": False,
        "enable_readlinkat": False,
        "enable_syscall": True,
        "enable_lazy_lock": False,
        "enable_debug_logging": False,
        "enable_initial_exec_tls": True,
        "enable_libdl": True,
        "enable_prof": False,
        "lg_page": "",
        "lg_hugepage": "",
        "malloc_conf": "",
    }

    @property
    def _library_name(self):
        libname = "jemalloc"
        if self.settings.os == "Windows":
            if not self.options.shared:
                libname += "_s"
        else:
            if not self.options.shared and self.options.fPIC:
                libname += "_pic"
        return libname

    def export_sources(self):
        export_conandata_patches(self)

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if not self.options.enable_cxx:
            self.settings.rm_safe("compiler.cppstd")
            self.settings.rm_safe("compiler.libcxx")

    def layout(self):
        basic_layout(self, src_folder="src")

    def build_requirements(self):
        self.tool_requires("automake/1.16.5")

    def validate(self):
        if is_msvc(self):
            raise ConanInvalidConfiguration("MSVC is not supported by this vendored recipe")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        env = VirtualBuildEnv(self)
        env.generate()
        tc = AutotoolsToolchain(self)
        enable_disable = lambda opt, val: f"--enable-{opt}" if val else f"--disable-{opt}"
        tc.configure_args.extend(
            [
                f"--with-jemalloc-prefix={self.options.prefix}",
                enable_disable("debug", self.settings.build_type == "Debug"),
                enable_disable("cxx", self.options.enable_cxx),
                enable_disable("fill", self.options.enable_fill),
                enable_disable("xmalloc", self.options.enable_xmalloc),
                enable_disable("readlinkat", self.options.enable_readlinkat),
                enable_disable("syscall", self.options.enable_syscall),
                enable_disable("lazy-lock", self.options.enable_lazy_lock),
                enable_disable("log", self.options.enable_debug_logging),
                enable_disable("initial-exec-tls", self.options.enable_initial_exec_tls),
                enable_disable("libdl", self.options.enable_libdl),
                enable_disable("prof", self.options.enable_prof),
            ]
        )
        if self.options.lg_page:
            tc.configure_args.append(f"--with-lg-page={self.options.lg_page}")
        if self.options.lg_hugepage:
            tc.configure_args.append(f"--with-lg-hugepage={self.options.lg_hugepage}")
        if self.options.malloc_conf:
            tc.configure_args.append(f"--with-malloc-conf={self.options.malloc_conf}")
        tc.generate()

    def build(self):
        apply_conandata_patches(self)
        autotools = Autotools(self)
        autotools.configure()
        autotools.make()

    def package(self):
        copy(self, pattern="COPYING*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        autotools = Autotools(self)
        autotools.install(target="install_lib_shared" if self.options.shared else "install_lib_static")
        autotools.install(target="install_include")

    def package_info(self):
        self.cpp_info.set_property("pkg_config_name", "jemalloc")
        self.cpp_info.libs = [self._library_name]
        self.cpp_info.includedirs = [
            os.path.join(self.package_folder, "include"),
            os.path.join(self.package_folder, "include", "jemalloc"),
        ]
        if not self.options.shared:
            self.cpp_info.defines = ["JEMALLOC_EXPORT="]
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.extend(["dl", "pthread", "rt"])
