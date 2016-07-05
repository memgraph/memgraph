#pragma once

#include <cassert>

#include "data_structures/skiplist/skiplist_map.hpp"
#include "utils/total_ordering.hpp"

template <class T, size_t H=32, class lock_t=SpinLock>
class SkipListSet
{
    class Key : public TotalOrdering<Key>
    {
    public:
        Key() = default;
        Key(const T* item) : item(item) {}
        Key(const T& item) : item(&item) {}

        friend constexpr bool operator<(const Key& lhs, const Key& rhs)
        {
            assert(!lhs.empty());
            assert(!rhs.empty());

            return *lhs.item < *rhs.item;
        }

        friend constexpr bool operator==(const Key& lhs, const Key& rhs)
        {
            assert(!lhs.empty());
            assert(!rhs.empty());

            return *lhs.item == *rhs.item;
        }

        bool empty() const
        {
            return item == nullptr;
        }

        operator const T&() const
        {
            assert(item != nullptr);
            return *item;
        }

    private:
        const T* item {nullptr};
    };

    using skiplist_t = SkipListMap<Key, T, H, lock_t>;
    using iter_t = typename skiplist_t::Iterator;
    using const_iter_t = typename skiplist_t::ConstIterator;

public:
    template <class Derived, class It>
    class IteratorBase : public Crtp<Derived>
    {
    protected:
        IteratorBase(const It& it) : it(it) {}
        IteratorBase(It&& it) : it(std::move(it)) {}

        It it;
    public:
        IteratorBase() = default;
        IteratorBase(const IteratorBase&) = default;

        auto& operator*()
        {
            return it->second;
        }

        auto* operator->()
        {
            return &it->second;
        }

        operator T&()
        {
            return it->second;
        }

        Derived& operator++()
        {
            it++;
            return this->derived();
        }

        Derived& operator++(int)
        {
            return operator++();
        }

        friend bool operator==(const Derived& a, const Derived& b)
        {
            return a.it == b.it;
        }

        friend bool operator!=(const Derived& a, const Derived& b)
        {
            return !(a == b);
        }
    };

    class ConstIterator : public IteratorBase<ConstIterator, const_iter_t>
    {
        friend class SkipListSet;

        using Base = IteratorBase<ConstIterator, const_iter_t>;

        ConstIterator(const const_iter_t& it) : Base(it) {}
        ConstIterator(const_iter_t&& it) : Base(std::move(it)) {}

    public:
        ConstIterator() = default;
        ConstIterator(const ConstIterator&) = default;

        const T& operator*()
        {
            return Base::operator*();
        }

        const T* operator->()
        {
            return Base::operator->();
        }

        operator const T&()
        {
            return Base::operator T&();
        }
    };

    class Iterator : public IteratorBase<Iterator, iter_t>
    {
        friend class SkipListSet;

        using Base = IteratorBase<Iterator, iter_t>;

        Iterator(const iter_t& it) : Base(it) {}
        Iterator(iter_t&& it) : Base(std::move(it)) {}

    public:
        Iterator() = default;
        Iterator(const Iterator&) = default;

    };

    SkipListSet() = default;

    friend class Accessor;

    class Accessor
    {
        friend class SkipListSet;

        using accessor_t = typename skiplist_t::Accessor;
        Accessor(accessor_t&& accessor) : accessor(std::move(accessor)) {}

    public:
        Accessor(const Accessor&) = delete;
        Accessor(Accessor&&) = default;

        Iterator begin()
        {
            return Iterator(accessor.begin());
        }

        ConstIterator begin() const
        {
            return ConstIterator(accessor.cbegin());
        }

        ConstIterator cbegin() const
        {
            return ConstIterator(accessor.cbegin());
        }

        Iterator end()
        {
            return Iterator(accessor.end());
        }

        ConstIterator end() const
        {
            return ConstIterator(accessor.cend());
        }

        ConstIterator cend() const
        {
            return ConstIterator(accessor.cend());
        }

        std::pair<Iterator, bool> insert(const T& item)
        {
            auto r = accessor.insert_unique(Key(item), item);
            return { Iterator(r.first), r.second };
        }

        ConstIterator find(const T& item) const
        {
            return ConstIterator(accessor.find(Key(item)));
        }

        Iterator find(const T& item)
        {
            return Iterator(accessor.find(Key(item)));
        }

        bool contains(const T& item) const
        {
            return accessor.find(Key(item)) != accessor.end();
        }

        bool remove(const T& item)
        {
            return accessor.remove(Key(item));
        }

    private:
        accessor_t accessor;
    };

    Accessor access()
    {
        return Accessor(std::move(data.access()));
    }

    Accessor access() const
    {
        return Accessor(std::move(data.access()));
    }

private:
    SkipListMap<Key, T> data;
};
