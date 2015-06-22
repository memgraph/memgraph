#include <iostream>

#include "utils/memory/allocator.hpp"
#include "utils/memory/maker.hpp"

struct TestStruct
{
    TestStruct(int a, int b, int c, int d)
        : a(a), b(b), c(c), d(d) {}

    int a, b, c, d;
};

void test_classic(int N)
{
    TestStruct** xs = new TestStruct*[N];

    for(int i = 0; i < N; ++i)
        xs[i] = new TestStruct(i, i, i, i);

    for(int i = 0; i < N; ++i)
        delete xs[i];

    delete[] xs;
}

void test_fast(int N)
{
    TestStruct** xs = makeme<TestStruct*>(N);

    for(int i = 0; i < N; ++i)
        xs[i] = makeme<TestStruct>(i, i, i, i);

    for(int i = 0; i < N; ++i)
        delete xs[i];

    delete[] xs;
}

int main(void)
{
    constexpr int N = 20000000;
    test_classic(N);
    test_fast(N);
    return 0;
}
