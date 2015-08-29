#include <iostream>

#include "pool.hpp"

int main(void)
{
    std::cout << "hardware_concurrency " << std::thread::hardware_concurrency() << std::endl;

    auto size = 2048;
    auto N = 1000000;

    Pool pool(size);

    for(int i = 0; i < N; ++i)
        pool.execute([size](int) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }, i);

    return 0;
}
