clang++ -O2 -DNDEBUG -std=c++14 client.cpp -o client.out -I../../ -pthread
clang++ -O2 -DNDEBUG -std=c++14 benchmark_json.cpp -o benchmark_json.out -I../../ -pthread
