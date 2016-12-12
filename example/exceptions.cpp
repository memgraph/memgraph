#include <iostream>

#include "utils/exceptions/basic_exception.hpp"

void i_will_throw()
{
    throw BasicException("this is not {}", "ok!");
}

void bar()
{
    i_will_throw();
}

void foo()
{
    bar();
}

int main(void)
{
    try
    {
        foo();
    }
    catch(std::exception& e)
    {
        std::cout << e.what() << std::endl;
    }

    return 0;
}
