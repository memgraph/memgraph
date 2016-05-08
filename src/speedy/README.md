## Dependencies

* https://github.com/libuv/libuv

```
sh autogen.sh
./configure
make
make check
make install
```

* https://github.com/joyent/http-parser

```
make
make install -stdlib=libstdc++ -lstdc++ (OSX)
make install
```

* https://github.com/c9s/r3 (git submodule)

```
./autogen.sh
./configure
make
```

* http://pcre.org/ (version 8.35, R3 dependency)

```
./configure
make
sudo make install

```

## NOTE
r3_include.h is custom r3 header file because of compilation problem related to redefinition of bool
