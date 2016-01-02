#pragma once

#include "utils/total_ordering.hpp"

#include "storage/indexes/sort_order.hpp"
#include "storage/model/properties/properties.hpp"
#include "storage/model/properties/property.hpp"

template <class K, class SortOrder>
class UniqueKey : public TotalOrdering<UniqueKey<K, SortOrder>>
{
public:
    using type = UniqueKey<K, SortOrder>;
    using key_t = K;

    UniqueKey(const K& key) : key(key) {}

    friend constexpr bool operator<(const type& lhs, const type& rhs)
    {
        return sort_order(lhs.key, rhs.key);
    }

    friend constexpr bool operator==(const type& lhs, const type& rhs)
    {
        return lhs.key == rhs.key;
    }

    operator const K&() const { return key; }

private:
    static constexpr SortOrder sort_order = SortOrder();
    const K& key;
};

template <class K>
using UniqueKeyAsc = UniqueKey<K, Ascending<K>>;

template <class K>
using UniqueKeyDesc = UniqueKey<K, Descending<K>>;

