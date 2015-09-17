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
brew install r3 (OSX)
```

## Test compile

```
clang++ -std=c++11 test.cpp -o test.out -luv -lhttp_parser -I../
```
