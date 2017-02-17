#pragma once

#include "data_structures/concurrent/skiplist.hpp"

// Multi thread safe multiset based on skiplist.
// T - type of data.
template <class T>
class ConcurrentMultiSet
{
    typedef SkipList<T> list;
    typedef typename SkipList<T>::Iterator list_it;
    typedef typename SkipList<T>::ConstIterator list_it_con;

public:
    ConcurrentMultiSet() {}

    class Accessor : public AccessorBase<T>
    {
        friend class ConcurrentMultiSet;

        using AccessorBase<T>::AccessorBase;

    private:
        using AccessorBase<T>::accessor;

    public:
        list_it insert(const T &item)
        {
            return accessor.insert_non_unique(item);
        }

        list_it insert(T &&item)
        {
            return accessor.insert_non_unique(std::forward<T>(item));
        }

        list_it_con find(const T &item) const { return accessor.find(item); }

        list_it find(const T &item) { return accessor.find(item); }

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

        bool contains(const T &item) const
        {
            return this->find(item) != this->end();
        }

        bool remove(const T &item) { return accessor.remove(item); }
    };

    Accessor access() { return Accessor(&skiplist); }

    const Accessor access() const { return Accessor(&skiplist); }

private:
    list skiplist;
};
