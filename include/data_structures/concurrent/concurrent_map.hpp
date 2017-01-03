#pragma once

#include "data_structures/concurrent/common.hpp"
#include "data_structures/concurrent/skiplist.hpp"

using std::pair;

/**
 * Multi thread safe map based on skiplist.
 *
 * @tparam K is a type of key.
 * @tparam T is a type of data.
 */
template <typename K, typename T>
class ConcurrentMap
{
    typedef Item<K, T> item_t;
    typedef SkipList<item_t> list;
    typedef typename SkipList<item_t>::Iterator list_it;
    typedef typename SkipList<item_t>::ConstIterator list_it_con;

public:
    ConcurrentMap() {}

    class Accessor : public AccessorBase<item_t>
    {
        friend class ConcurrentMap;

        using AccessorBase<item_t>::AccessorBase;

    private:
        using AccessorBase<item_t>::accessor;

    public:
        std::pair<list_it, bool> insert(const K &key, const T &data)
        {
            return accessor.insert(item_t(key, data));
        }

        std::pair<list_it, bool> insert(const K &key, T &&data)
        {
            return accessor.insert(item_t(key, std::move(data)));
        }

        std::pair<list_it, bool> insert(K &&key, T &&data)
        {
            return accessor.insert(
                item_t(std::forward<K>(key), std::forward<T>(data)));
        }

        template <class... Args1, class... Args2>
        std::pair<list_it, bool> emplace(const K &key,
                                         std::tuple<Args1...> first_args,
                                         std::tuple<Args2...> second_args)
        {
            return accessor.emplace(
                key, std::piecewise_construct,
                std::forward<std::tuple<Args1...>>(first_args),
                std::forward<std::tuple<Args2...>>(second_args));
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
