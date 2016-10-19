#include <iostream>
#include <vector>

#include "utils/iterator/map.hpp"

int main(void)
{
    std::vector<int> test{1,2,3};

    for (auto item : test) {
        std::cout << item << std::endl;
    }

    return 0;
}
