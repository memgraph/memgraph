#pragma once

#include "data_structures/skiplist/skiplist.hpp"
#include "keys/unique_key.hpp"

#include "storage/cursor.hpp"

template <class Key, class Item>
class Index
{
public:
    using skiplist_t = SkipList<Key, Item*>;
    using iterator_t = typename skiplist_t::Iterator;
    using accessor_t = typename skiplist_t::Accessor;
    using K = typename Key::key_t;
    using cursor_t = Cursor<accessor_t, iterator_t, K>;

    // cursor_t insert(const K& key, Item* item, tx::Transaction& t)
    auto insert(const K& key, Item* item)
    {
        // Item has to be some kind of container for the real data like Vertex,
        // the container has to be transactionally aware
        // in other words index or something that wraps index has to be
        // transactionally aware

        auto accessor = skiplist.access();
        auto result = accessor.insert_unique(key, item);

        // TODO: handle existing insert

        return result;
    }

    auto remove(const K& key)
    {
        auto accessor = skiplist.access();
        return accessor.remove(key);
    }

    auto find(const K& key)
    {
        auto accessor = skiplist.access();
        return accessor.find(key);
    }
    
private:
    skiplist_t skiplist;
};
