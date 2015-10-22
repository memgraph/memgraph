## Dynamic Code

```
cd example
clang++ -std=c++1y mysql.cpp -o ../tmp/mysql.so -shared -fPIC
clang++ -std=c++1y test.cpp -o test.out -ldl
```
