#include <iostream>

#include "data_structures/skiplist/skiplist.hpp"


int main(void)
{
    xorshift::init();
    SkipList<int, int> skiplist;

    int k1 = 1;
    int i1 = 10;

    int k2 = 2;
    int i2 = 20;

    std::cout << "find: " << k1 << " => " << (skiplist.find(&k1) == nullptr) << std::endl;
    std::cout << skiplist.insert(&k1, &i1) << std::endl;
    std::cout << "find: " << k1 << " => " << *skiplist.find(&k1)->item << std::endl;
    std::cout << "find: " << k2 << " => " << (skiplist.find(&k2) == nullptr) << std::endl;
    std::cout << skiplist.insert(&k2, &i2) << std::endl;
    std::cout << skiplist.insert(&k2, &i2) << std::endl;
    std::cout << skiplist.insert(&k1, &i1) << std::endl;
    std::cout << skiplist.insert(&k2, &i2) << std::endl;
    std::cout << "DELETE K1 " << skiplist.remove(&k1) << std::endl;
    std::cout << "find: " << k1 << " => " << (skiplist.find(&k1) == nullptr) << std::endl;
    std::cout << "find: " << k2 << " => " << *skiplist.find(&k2)->item << std::endl;
    
    std::cout << skiplist.size() << std::endl;

    auto* node = skiplist.header.load(std::memory_order_consume);

    for(size_t i = 0; i < skiplist.size(); ++i)
    {
        node = node->forward(0);

        if(node == nullptr)
            std::cout << "node == nullptr" << std::endl;

        if(node->key == nullptr)
            std::cout << "node->key == nullptr" << std::endl;

        if(node->item == nullptr)
            std::cout << "node->item == nullptr" << std::endl;

        std::cout << *node->key << " => " << *node->item << std::endl;
    }

    return 0;
}
