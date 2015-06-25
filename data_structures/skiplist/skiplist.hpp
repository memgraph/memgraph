#ifndef MEMGRAPH_DATA_STRUCTURES_SKIPLIST_SKIPLIST_HPP
#define MEMGRAPH_DATA_STRUCTURES_SKIPLIST_SKIPLIST_HPP

#include <algorithm>
#include <cstdlib>
#include <array>

#include "new_height.hpp"
#include "skipnode.hpp"

template <class K, class T>
class SkipList
{
    using Node = SkipNode<K, T>;

public:
    SkipList(size_t max_height);

    T* get(const K* const key);
    void put(const K* key, T* item);
    void del(const K* const key);

private:
    size_t level;
    Node* header;
};


template <class K, class T>
SkipList<K, T>::SkipList(size_t level)
    : level(level)
{
    header = new Node(level);
    auto sentinel = new Node();

    for(int i = 0; i < level; ++i)
        header->forward[i] = sentinel;
}

template <class K, class T>
T* SkipList<K, T>::get(const K* const key)
{
    Node* current = header;

    for(int i = level - 1; i >= 0; --i)
    {
        Node* next = current->forward[i];

        while(next->key != nullptr && *next->key < *key)
            current = current->forward[i];
    }

    return current->item;
}

template <class K, class T>
void SkipList<K, T>::put(const K* key, T* item)
{
    auto height = new_height(level);
    auto node = new Node(key, item, height);

    // needed to update higher level forward pointers
    int trace[level];

    Node* current = header;

    for(int i = level - 1; i >= 0; --i)
    {
        Node* next = current->forward[i];

        while(next->key != nullptr && *next->key < *key)
            current = current->forward[i];
    }
}

template <class K, class T>
void SkipList<K, T>::del(const K* const key)
{

}

#endif
