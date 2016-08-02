#pragma once

#include "data_structures/concurrent/skiplist.hpp"
#include "utils/total_ordering.hpp"

using std::pair;

template <typename K, typename T>
class Item : public TotalOrdering<Item<K, T>>,
             public TotalOrdering<K, Item<K, T>>,
             public TotalOrdering<Item<K, T>, K>,
             public pair<const K, T>
{
public:
    using pair<const K, T>::pair;

    friend constexpr bool operator<(const Item &lhs, const Item &rhs)
    {
        std::pair<const K, T> *a;
        return lhs.first < rhs.first;
    }

    friend constexpr bool operator==(const Item &lhs, const Item &rhs)
    {
        return lhs.first == rhs.first;
    }

    friend constexpr bool operator<(const K &lhs, const Item &rhs)
    {
        return lhs < rhs.first;
    }

    friend constexpr bool operator==(const K &lhs, const Item &rhs)
    {
        return lhs == rhs.first;
    }

    friend constexpr bool operator<(const Item &lhs, const K &rhs)
    {
        return lhs.first < rhs;
    }

    friend constexpr bool operator==(const Item &lhs, const K &rhs)
    {
        return lhs.first == rhs;
    }
};

template <typename T>
class AccessorBase
{
    typedef SkipList<T> list;
    typedef typename SkipList<T>::Iterator list_it;
    typedef typename SkipList<T>::ConstIterator list_it_con;

protected:
    AccessorBase(list *skiplist) : accessor(skiplist->access()) {}

public:
    AccessorBase(const AccessorBase &) = delete;

    AccessorBase(AccessorBase &&other) : accessor(std::move(other.accessor)) {}

    ~AccessorBase() {}

    list_it begin() { return accessor.begin(); }

    list_it_con begin() const { return accessor.cbegin(); }

    list_it_con cbegin() const { return accessor.cbegin(); }

    list_it end() { return accessor.end(); }

    list_it_con end() const { return accessor.cend(); }

    list_it_con cend() const { return accessor.cend(); }

    template <class K>
    typename SkipList<T>::template MultiIterator<K> end(const K &data)
    {
        return accessor.template mend<K>(data);
    }

    template <class K>
    typename SkipList<T>::template MultiIterator<K> mend(const K &data)
    {
        return accessor.template mend<K>(data);
    }

    size_t size() const { return accessor.size(); }

protected:
    typename list::Accessor accessor;
};
