#include <iostream>

#include "utils/terminate_handler.hpp"

int main()
{
    std::set_terminate(&terminate_handler);

    throw std::runtime_error("runtime error");

    return 0;
}
