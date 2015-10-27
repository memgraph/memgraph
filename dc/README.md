## Dynamic Code

```
man nm

cd memgraph/dc
cd example
clang++ -std=c++1y mysql.cpp -o ../tmp/mysql.so -shared -fPIC
clang++ -std=c++1y memsql.cpp -o ../tmp/memsql.so -shared -fPIC
cd ..
clang++ -std=c++1y test.cpp -o test.out -ldl
```
