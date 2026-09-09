from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.env import VirtualBuildEnv
from conan.tools.gnu import Autotools, AutotoolsToolchain
from conan.tools.layout import basic_layout
from conan.tools.files import export_conandata_patches, apply_conandata_patches, get, copy, rename, rmdir, trim_conandata
from conan.tools.microsoft import is_msvc, is_msvc_static_runtime
from conan.tools.scm import Version
import os
import shutil

required_conan_version = ">=1.54.0"


class JemallocConan(ConanFile):
    name = "jemalloc"
    description = "jemalloc is a general purpose malloc(3) implementation that emphasizes fragmentation avoidance and scalable concurrency support."
    url = "https://github.com/conan-io/conan-center-index"
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
    def _minimum_compilers_version(self):
        return {
            "clang": "3.9",
            "apple-clang": "8",
            # The upstream repository provides solution files for Visual Studio 2015, 2017, 2019 and 2022,
            # but the 2015 solution does not work properly due to unresolved external symbols:
            # `test_hooks_libc_hook` and `test_hooks_arena_new_hook`
            "Visual Studio": "15",
            "msvc": "191",
        }

    @property
    def _settings_build(self):
        return getattr(self, "settings_build", self.settings)

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

    def export(self):
        # Stabilise recipe revision across conan export and local-recipes-index
        trim_conandata(self)

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
        if self._settings_build.os == "Windows":
            self.win_bash = True
            if not self.conf.get("tools.microsoft.bash:path", check_type=str):
                self.tool_requires("msys2/cci.latest")

    def validate(self):
        minimum_version = self._minimum_compilers_version.get(str(self.settings.compiler), False)
        if minimum_version and Version(self.settings.compiler.version) < minimum_version:
            raise ConanInvalidConfiguration(f"{self.ref} requires {self.settings.compiler} >= {minimum_version}")
        # 1. MSVC specific checks
        if is_msvc(self):
            # Building the shared library with a static MSVC runtime is not supported
            if self.options.shared and is_msvc_static_runtime(self):
                raise ConanInvalidConfiguration("Building the shared library with MT runtime is not supported.")
            # Only x86-64 and x86 are supported
            if self.settings.arch not in ["x86_64", "x86"]:
                raise ConanInvalidConfiguration(f"{self.settings.arch} is not supported.")
        # 2. Clang specific checks
        if self.settings.compiler == "clang":
            if self.options.enable_cxx and self.settings.compiler.get_safe("libcxx") == "libc++" and \
                    Version(self.settings.compiler.version) < "10":
                raise ConanInvalidConfiguration("Clang 9 or earlier with libc++ is not supported due to the missing mutex implementation.")
        # 3: Apple Silicon specific checks
        if self.settings.os == "Macos" and self.settings.arch == "armv8":
            if Version(self.version) < "5.3.0":
                raise ConanInvalidConfiguration("Support for Apple Silicon is only available as of 5.3.0.")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        env = VirtualBuildEnv(self)
        env.generate()
        tc = AutotoolsToolchain(self)
        enable_disable = lambda opt, val: f"--enable-{opt}" if val else f"--disable-{opt}"
        tc.configure_args.extend([
            f"--with-jemalloc-prefix={self.options.prefix}",
            enable_disable("debug", self.settings.build_type == "Debug"),
            enable_disable("cxx", self.options.enable_cxx),
            enable_disable("fill", self.options.enable_fill),
            enable_disable("xmalloc", self.options.enable_cxx),
            enable_disable("readlinkat", self.options.enable_readlinkat),
            enable_disable("syscall", self.options.enable_syscall),
            enable_disable("lazy-lock", self.options.enable_lazy_lock),
            enable_disable("log", self.options.enable_debug_logging),
            enable_disable("initial-exec-tls", self.options.enable_initial_exec_tls),
            enable_disable("libdl", self.options.enable_libdl),
            enable_disable("prof", self.options.enable_prof),
        ])
        if self.options.lg_page:
            tc.configure_args.append(f"--with-lg-page={self.options.lg_page}")
        if self.options.lg_hugepage:
            tc.configure_args.append(f"--with-lg-hugepage={self.options.lg_hugepage}")
        if self.options.malloc_conf:
            tc.configure_args.append(f"--with-malloc-conf={self.options.malloc_conf}")
        env = tc.environment()
        if is_msvc(self):
            # Do not check whether the math library exists when compiled by MSVC
            # because MSVC treats the function `char log()` as a intrinsic function
            # and therefore complains about insufficient arguments passed to the function
            tc.configure_args.append("ac_cv_search_log=none required")
            env.define("CC", "cl")
            env.define("CXX", "cl")
        tc.generate(env)

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
        if self.settings.os == "Windows" and self.settings.compiler == "gcc":
            rename(self, os.path.join(self.package_folder, "lib", f"{self._library_name}.lib"),
                         os.path.join(self.package_folder, "lib", f"lib{self._library_name}.a"))
            if not self.options.shared:
                os.unlink(os.path.join(self.package_folder, "lib", "jemalloc.lib"))
        if is_msvc(self):
            shutil.copytree(os.path.join(self.source_folder, "include", "msvc_compat"),
                            os.path.join(self.package_folder, "include", "msvc_compat"))
            if self.options.shared:
                rmdir(self, os.path.join(self.package_folder, "lib"))
                copy(self, "*.lib", os.path.join(self.build_folder, "lib"), os.path.join(self.package_folder, "lib"))
                copy(self, "*.dll", os.path.join(self.build_folder, "lib"), os.path.join(self.package_folder, "bin"))

    def package_info(self):
        self.cpp_info.set_property("pkg_config_name", "jemalloc")
        self.cpp_info.libs = [self._library_name]
        self.cpp_info.includedirs = [os.path.join(self.package_folder, "include"),
                                     os.path.join(self.package_folder, "include", "jemalloc")]
        if is_msvc(self):
            self.cpp_info.includedirs.append(os.path.join(self.package_folder, "include", "msvc_compat"))
        if not self.options.shared:
            self.cpp_info.defines = ["JEMALLOC_EXPORT="]
        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.extend(["dl", "pthread", "rt"])
