import os

from conan import ConanFile
from conan.tools.files import copy, get, rename, replace_in_file
from conan.tools.gnu import AutotoolsToolchain
from conan.tools.layout import basic_layout


class LibbcryptConan(ConanFile):
    name = "libbcrypt"
    version = "1.0-memgraph"
    description = "bcrypt password hashing library."
    url = "https://github.com/memgraph/memgraph"
    license = "ISC"
    homepage = "https://github.com/rg3/libbcrypt"
    topics = ("bcrypt", "password", "hashing")
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

    def layout(self):
        basic_layout(self, src_folder="src")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = AutotoolsToolchain(self)
        tc.generate()

    def build(self):
        # Remove -Wcast-align which causes errors with Clang
        crypt_blowfish_makefile = os.path.join(self.source_folder, "crypt_blowfish", "Makefile")
        replace_in_file(self, crypt_blowfish_makefile, "-Wcast-align", "")

        # The upstream Makefile hardcodes its own CFLAGS, so AutotoolsToolchain env vars
        # (including sanitizer flags) don't reach the compilation. This is acceptable:
        # libbcrypt is a static library, and when linked into an ASAN binary the ASAN
        # runtime handles interception at the process level.
        cc = self.conf.get("tools.build:compiler_executables", default={}).get("c", "cc")
        self.run(f"make -C {self.source_folder} CC={cc}")

    def package(self):
        copy(self, "COPYING", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        copy(self, "bcrypt.a", src=self.source_folder, dst=os.path.join(self.package_folder, "lib"))
        rename(
            self,
            os.path.join(self.package_folder, "lib", "bcrypt.a"),
            os.path.join(self.package_folder, "lib", "libbcrypt.a"),
        )
        copy(self, "bcrypt.h", src=self.source_folder, dst=os.path.join(self.package_folder, "include", "libbcrypt"))

    def package_info(self):
        self.cpp_info.libs = ["bcrypt"]
