#pragma once

#include <algorithm>
#include <memory>
#include <cassert>

#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

#include "utils/random/fast_binomial.hpp"
#include "memory/lazy_gc.hpp"

// concurrent skiplist based on the implementation described in
// "A Provably Correct Scalable Concurrent Skip List"
// https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf

template <class K, class T, size_t H=32, class lock_t=SpinLock>
class SkipList : LazyGC<SkipList<K, T, H, lock_t>>, Lockable<lock_t>
{
public:
    static thread_local FastBinomial<H> rnd;

    using data_t = std::pair<const K, T>;

    struct Flags
    {
        enum node_flags : uint8_t {
            MARKED       = 0x01,
            FULLY_LINKED = 0x10,
        };

        bool is_marked() const
        {
            return flags.load(std::memory_order_acquire) & MARKED;
        }

        void set_marked()
        {
            flags.fetch_or(MARKED, std::memory_order_release);
        }

        bool is_fully_linked() const
        {
            return flags.load(std::memory_order_acquire) & FULLY_LINKED;
        }

        void set_fully_linked()
        {
            flags.fetch_or(FULLY_LINKED, std::memory_order_release);
        }

    private:
        std::atomic<uint8_t> flags {0};
    };

    class Node : Lockable<lock_t>
    {
    public:
        friend class SkipList;

        data_t data;

        const uint8_t height;
        Flags flags;

        static Node* create(const K& key, const T& item, uint8_t height)
        {
            return create({key, item}, height);
        }

        static Node* create(const K& key, T&& item, uint8_t height)
        {
            return create({key, std::move(item)}, height);
        }

        static Node* create(data_t&& data, uint8_t height)
        {
            // [      Node      ][Node*][Node*][Node*]...[Node*]
            //         |            |      |      |         |
            //         |            0      1      2      height-1
            // |----------------||-----------------------------|
            //   space for Node     space for tower pointers
            //     structure          right after the Node
            //                             structure
            auto size = sizeof(Node) + height * sizeof(std::atomic<Node*>);
            auto node = static_cast<Node*>(std::malloc(size));

            // we have raw memory and we need to construct an object
            // of type Node on it
            return new (node) Node(std::move(data), height);
        }

        static void destroy(Node* node)
        {
            node->~Node();
            free(node);
        }

        Node* forward(size_t level) const
        {
            return tower[level].load(std::memory_order_acquire);
        }

        void forward(size_t level, Node* next)
        {
            tower[level].store(next, std::memory_order_release);
        }

    private:
        Node(data_t&& data, uint8_t height)
            : data(std::move(data)), height(height)
        {
            // here we assume, that the memory for N towers (N = height) has
            // been allocated right after the Node structure so we need to
            // initialize that memory
            for(auto i = 0; i < height; ++i)
                new (&tower[i]) std::atomic<Node*> {nullptr};
        }

        ~Node()
        {
            for(auto i = 0; i < height; ++i)
                tower[i].~atomic();
        }

        // this creates an array of the size zero. we can't put any sensible
        // value here since we don't know what size it will be untill the
        // node is allocated. we could make it a Node** but then we would
        // have two memory allocations, one for node and one for the forward
        // list. this way we avoid expensive malloc/free calls and also cache
        // thrashing when following a pointer on the heap
        std::atomic<Node*> tower[0];
    };

public:
    class ConstIterator
    {
        friend class SkipList;
        ConstIterator(Node* node) : node(node) {}

    public:
        ConstIterator() = default;
        ConstIterator(const ConstIterator&) = default;

        const data_t& operator*()
        {
            assert(node != nullptr);
            return node->data;
        }

        const data_t* operator->()
        {
            assert(node != nullptr);
            return &node->data;
        }

        operator const data_t()
        {
            assert(node != nullptr);
            return node->data;
        }

        ConstIterator& operator++()
        {
            assert(node != nullptr);
            node = node->forward(0);
            return *this;
        }

        ConstIterator& operator++(int)
        {
            return operator++();
        }

        friend bool operator==(const ConstIterator& a, const ConstIterator& b)
        {
            return a.node == b.node;
        }

        friend bool operator!=(const ConstIterator& a, const ConstIterator& b)
        {
            return !(a == b);
        }

    private:
        Node* node {nullptr};
    };

    class Iterator
    {
        friend class SkipList;
        Iterator(Node* node) : node(node) {}

    public:
        Iterator() = default;
        Iterator(const Iterator&) = default;

        data_t& operator*()
        {
            assert(node != nullptr);
            return node->data;
        }

        data_t* operator->()
        {
            assert(node != nullptr);
            return &node->data;
        }

        operator data_t()
        {
            assert(node != nullptr);
            return node->data;
        }

        Iterator& operator++()
        {
            assert(node != nullptr);
            node = node->forward(0);
            return *this;
        }

        Iterator& operator++(int)
        {
            return operator++();
        }

        friend bool operator==(const Iterator& a, const Iterator& b)
        {
            return a.node == b.node;
        }

        friend bool operator!=(const Iterator& a, const Iterator& b)
        {
            return !(a == b);
        }

    private:
        Node* node {nullptr};
    };

    SkipList() : header(Node::create(K(), std::move(T()), H)) {}

    friend class Accessor;

    class Accessor
    {
        friend class SkipList;

        Accessor(SkipList& skiplist) : skiplist(skiplist) {}
    public:
        Accessor(const Accessor&) = delete;
        Accessor(Accessor&&) = default;

        Iterator begin()
        {
            return skiplist.begin();
        }

        ConstIterator begin() const
        {
            return skiplist.cbegin();
        }

        ConstIterator cbegin() const
        {
            return skiplist.cbegin();
        }

        Iterator end()
        {
            return skiplist.end();
        }

        ConstIterator end() const
        {
            return skiplist.cend();
        }

        ConstIterator cend() const
        {
            return skiplist.cend();
        }

        std::pair<Iterator, bool> insert_unique(const K& key, const T& item)
        {
            return skiplist.insert({key, item}, preds, succs);
        }

        std::pair<Iterator, bool> insert_unique(const K& key, T&& item)
        {
            return skiplist.insert({key, std::move(item)}, preds, succs);
        }

        ConstIterator find(const K& key) const
        {
            return skiplist.find(key);
        }

        Iterator find(const K& key)
        {
            return skiplist.find(key);
        }

        bool remove(const K& key)
        {
            return skiplist.remove(key, preds, succs);
        }

    private:
        SkipList& skiplist;
        Node* preds[H], *succs[H];
    };

    Accessor access()
    {
        return Accessor(*this);
    }

private:
    using guard_t = std::unique_lock<lock_t>;

    Iterator begin()
    {
        return Iterator(header->forward(0));
    }

    ConstIterator begin() const
    {
        return ConstIterator(header->forward(0));
    }

    ConstIterator cbegin() const
    {
        return ConstIterator(header->forward(0));
    }

    Iterator end()
    {
        return Iterator();
    }

    ConstIterator end() const
    {
        return ConstIterator();
    }

    ConstIterator cend() const
    {
        return ConstIterator();
    }

    size_t size() const
    {
        return count.load(std::memory_order_acquire);
    }

    bool greater(const K& key, const Node* const node)
    {
        return node && key > node->data.first;
    }

    bool less(const K& key, const Node* const node)
    {
        return (node == nullptr) || key < node->data.first;
    }

    ConstIterator find(const K& key) const
    {
        return find_node<ConstIterator>(key);
    }

    Iterator find(const K& key)
    {
        return find_node<Iterator>(key);
    }

    template <class It>
    It find_node(const K& key)
    {
        Node* node, *pred = header;
        int h = static_cast<int>(pred->height) - 1;

        while(true)
        {
            // try to descend down first the next key on this layer overshoots
            for(; h >= 0 && less(key, node = pred->forward(h)); --h) {}

            // if we overshoot at every layer, item doesn't exist
            if(h < 0)
                return It();

            // the item is farther to the right, continue going right as long
            // as the key is greater than the current node's key
            while(greater(key, node))
                pred = node, node = node->forward(h);

            // check if we have a hit. if not, we need to descend down again
            if(!less(key, node) && !node->flags.is_marked())
                return It(node);
        }
    }

    int find_path(Node* from, int start, const K& key,
                  Node* preds[], Node* succs[])
    {
        int level_found = -1;
        Node* pred = from;

        for(int level = start; level >= 0; --level)
        {
            Node* node = pred->forward(level);

            while(greater(key, node))
                pred = node, node = pred->forward(level);

            if(level_found == -1 && !less(key, node))
                level_found = level;

            preds[level] = pred;
            succs[level] = node;
        }

        return level_found;
    }

    template <bool ADDING>
    bool lock_nodes(uint8_t height, guard_t guards[],
                    Node* preds[], Node* succs[])
    {
        Node *prepred, *pred, *succ = nullptr;
        bool valid = true;

        for(int level = 0; valid && level < height; ++level)
        {
            pred = preds[level], succ = succs[level];

            if(pred != prepred)
                guards[level] = pred->acquire_unique(), prepred = pred;

            valid = !pred->flags.is_marked() && pred->forward(level) == succ;

            if(ADDING)
                valid = valid && (succ == nullptr || !succ->flags.is_marked());
        }

        return valid;
    }

    std::pair<Iterator, bool>
        insert(data_t&& data, Node* preds[], Node* succs[])
    {
        while(true)
        {
            auto level = find_path(header, H - 1, data.first, preds, succs);

            if(level != -1)
            {
                auto found = succs[level];

                if(found->flags.is_marked())
                    continue;

                while(!found->flags.is_fully_linked())
                    usleep(250);

                return {Iterator {succs[level]}, false};
            }

            auto height = rnd();
            guard_t guards[H];

            // try to acquire the locks for predecessors up to the height of
            // the new node. release the locks and try again if someone else
            // has the locks
            if(!lock_nodes<true>(height, guards, preds, succs))
                continue;

            // you have the locks, create a new node
            auto new_node = Node::create(std::move(data), height);

            // link the predecessors and successors, e.g.
            //
            // 4 HEAD ... P ------------------------> S ... NULL
            // 3 HEAD ... ... P -----> NEW ---------> S ... NULL
            // 2 HEAD ... ... P -----> NEW -----> S ... ... NULL
            // 1 HEAD ... ... ... P -> NEW -> S ... ... ... NULL
            for(uint8_t level = 0; level < height; ++level)
            {
                new_node->forward(level, succs[level]);
                preds[level]->forward(level, new_node);
            }

            new_node->flags.set_fully_linked();
            count.fetch_add(1, std::memory_order_relaxed);

            return {Iterator {new_node}, true};
        }
    }

    bool ok_delete(Node* node, int level)
    {
        return node->flags.is_fully_linked()
            && node->height - 1 == level
            && !node->flags.is_marked();
    }

    bool remove(const K& key, Node* preds[], Node* succs[])
    {
        Node* node = nullptr;
        guard_t node_guard;
        bool marked = false;
        int height = 0;

        while(true)
        {
            auto level = find_path(header, H - 1, key, preds, succs);

            if(!marked && (level == -1 || !ok_delete(succs[level], level)))
                    return false;

            if(!marked)
            {
                node = succs[level];
                height = node->height;
                node_guard = node->acquire_unique();

                if(node->flags.is_marked())
                    return false;

                node->flags.set_marked();
            }

            guard_t guards[H];

            if(!lock_nodes<false>(height, guards, preds, succs))
                continue;

            for(int level = height - 1; level >= 0; --level)
                preds[level]->forward(level, node->forward(level));

            // TODO recycle(node)
            count.fetch_sub(1, std::memory_order_relaxed);
            return true;
        }
    }

    guard_t gc_lock_acquire()
    {
        return this->acquire_unique();
    }

    void vacuum()
    {

    }

    std::atomic<size_t> count {0};
    Node* header;
};

template <class K, class T, size_t H, class lock_t>
thread_local FastBinomial<H> SkipList<K, T, H, lock_t>::rnd;

