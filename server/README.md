## Dependencies

* https://github.com/libuv/libuv

```{engine='bash'}
sh autogen.sh
./configure
make
make check
make install
```

* https://github.com/joyent/http-parser

```{engine='bash'}
make
make install -stdlib=libstdc++ -lstdc++ (OSX)
make install
```

## Test compile

```{engine='bash'}
clang++ -std=c++11 test.cpp -o test -luv -lhttp_parser -I../
```