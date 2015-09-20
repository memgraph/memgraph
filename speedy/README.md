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

* https://github.com/c9s/r3

```
./autogen.sh
./configure
make
sudo make install
```

## NOTE
r3_include.h is custom r3 header file because of compilation problem related to redefinition of bool
