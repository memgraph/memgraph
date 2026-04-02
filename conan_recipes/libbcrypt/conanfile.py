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

        self.run(
            f"make -C {self.source_folder} CC={self.conf.get('tools.build:compiler_executables').get('c', 'cc')}",
        )

    def package(self):
        copy(self, "COPYING", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        copy(self, "bcrypt.a", src=self.source_folder, dst=os.path.join(self.package_folder, "lib"))
        rename(
            self,
            os.path.join(self.package_folder, "lib", "bcrypt.a"),
            os.path.join(self.package_folder, "lib", "libbcrypt.a"),
        )
        copy(self, "bcrypt.h", src=self.source_folder, dst=os.path.join(self.package_folder, "include"))

    def package_info(self):
        self.cpp_info.libs = ["bcrypt"]
