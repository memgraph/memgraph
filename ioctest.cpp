#include <iostream>

#include "utils/ioc/container.hpp"

struct A
{
    A()
    {
        std::cout << "Constructor of A" << std::endl;
    }

    int a = 3;
};

struct C;

struct B
{
    B(std::shared_ptr<A>&& a, std::shared_ptr<C>&& c)
        : a(a), c(c)
    {
        std::cout << "Constructor of B" << std::endl;
    }

    int b = 4;

    std::shared_ptr<A> a;
    std::shared_ptr<C> c;
};

struct C
{
    C(int c) : c(c)
    {
        std::cout << "Constructor of C" << std::endl;
    }

    int c;
};

int main(void)
{
    ioc::Container container;

    // register a singleton class A
    auto a = container.singleton<A>();
    std::cout << a->a << std::endl; // should print 3

    // register a factory function that makes C
    container.factory<C>([]() {
        return std::make_shared<C>(5);
    });

    // try to resolve A
    auto aa = container.resolve<A>();
    std::cout << aa->a << std::endl; // should print 3

    // register a singleton class B with dependencies A and C
    // (A will be resolved as an existing singleton and C will
    // be created by the factory function defined above)
    auto b = container.singleton<B, A, C>();
    std::cout << b->b << std::endl; // should print 4
    std::cout << b->a->a << std::endl; // should print 3
    std::cout << b->c->c << std::endl; // should print 5

    // try to resolve B
    auto bb = container.resolve<B>();
    std::cout << bb->b << std::endl; // should print 4

    // try to resolve C
    // (will be created as a new instance by the factory
    // function defined above)
    auto c = container.resolve<C>();
    std::cout << c->c << std::endl; // should print 5

    return 0;
};
