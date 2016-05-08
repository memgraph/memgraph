#include <iostream>

class A
{
public:
    class B;

};

class A::B
{
public:

};

int main(void)
{
    A a;
    A::B b;



    return 0;
}
