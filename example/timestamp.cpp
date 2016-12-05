#include <iostream>
#include <chrono>
#include <thread>

#include "utils/datetime/timestamp.hpp"

int main(void)
{
    auto timestamp = Timestamp::now();

    std::cout << timestamp << std::endl;
    std::cout << Timestamp::now() << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    std::cout << Timestamp::now().to_iso8601() << std::endl;

    std::cout << std::boolalpha;

    std::cout << (timestamp == Timestamp::now()) << std::endl;

    return 0;
}
