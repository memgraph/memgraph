#pragma once

#include "transactions/transaction.hpp"
#include "mvcc/version_list.hpp"
#include "storage/model/properties/property.hpp"

#include "storage/vertex.hpp"

template <class T, class Accessor, class It, class Derived>
class RecordCursor : public Crtp<Derived>
{
public:
    RecordCursor(Accessor&& accessor, It item, tx::Transaction& t)
        : accessor(accessor), item(item), t(t)
    {

    }

    // |------------------------- data operations -------------------------|

    const Property* property(const std::string& key) const
    {
        return record->props.at(key);
    }

    template <class V, class... Args>
    void property(const std::string& key, Args&&... args)
    {
        record->props.template set<V>(key, std::forward<Args>(args)...);
    }

    // |------------------------ record operations ------------------------|

    Derived& update()
    {
        record = item->update(t);
        return this->derived();
    }

    Derived& remove()
    {
        item->remove(t);
        return this->derived();
    }

    // |-------------------------- iterator impl --------------------------|

    Derived& operator++()
    {
        ++item;
    }

    Derived& operator++(int)
    {
        return operator++();
    }

    friend constexpr bool operator==(const Derived& a, const Derived& b)
    {
        return a.record == b.record;
    }

    friend constexpr bool operator!=(const Derived& a, const Derived& b)
    {
        return !(a == b);
    }

    const T& operator*() const
    {
        assert(record != nullptr);
        return *record;
    }

    const T* operator->() const
    {
        assert(record != nullptr);
        return record;
    }

    operator const T&() const
    {
        return *record;
    }

protected:
    Accessor accessor;
    tx::Transaction& t;
    It item;

    T* record;
};

template <class It, class Accessor>
class Vertex::Cursor
    : public RecordCursor<Vertex, Accessor, It, Cursor<It, Accessor>>
{
public:

};
