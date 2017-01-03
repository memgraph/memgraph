#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/total_ordering.hpp"

using std::pair;

/**
 * Multi thread safe multi map based on skiplist.
 *
 * @tparam K is a type of key.
 * @tparam T is a type of data.
 */
template <typename K, typename T>
class ConcurrentMultiMap
{
    typedef Item<K, T> item_t;
    typedef SkipList<item_t> list;
    typedef typename SkipList<item_t>::Iterator list_it;
    typedef typename SkipList<item_t>::ConstIterator list_it_con;
    typedef typename SkipList<item_t>::template MultiIterator<K> list_it_multi;

public:
    ConcurrentMultiMap() {}

    class Accessor : public AccessorBase<item_t>
    {
        friend class ConcurrentMultiMap<K, T>;

        using AccessorBase<item_t>::AccessorBase;

    private:
        using AccessorBase<item_t>::accessor;

    public:
        list_it insert(const K &key, const T &data)
        {
            return accessor.insert_non_unique(item_t(key, data));
        }

        list_it insert(const K &key, T &&data)
        {
            return accessor.insert_non_unique(
                item_t(key, std::forward<T>(data)));
        }

        list_it insert(K &&key, T &&data)
        {
            return accessor.insert_non_unique(
                item_t(std::forward<K>(key), std::forward<T>(data)));
        }

        list_it_multi find_multi(const K &key)
        {
            return accessor.find_multi(key);
        }

        list_it_con find(const K &key) const { return accessor.find(key); }

        list_it find(const K &key) { return accessor.find(key); }

        // Returns iterator to item or first larger if it doesn't exist.
        list_it_con find_or_larger(const T &item) const
        {
            return accessor.find_or_larger(item);
        }

        // Returns iterator to item or first larger if it doesn't exist.
        list_it find_or_larger(const T &item)
        {
            return accessor.find_or_larger(item);
        }

        bool contains(const K &key) const
        {
            return this->find(key) != this->end();
        }

        bool remove(const K &key) { return accessor.remove(key); }
    };

    Accessor access() { return Accessor(&skiplist); }

    const Accessor access() const { return Accessor(&skiplist); }

private:
    list skiplist;
};
