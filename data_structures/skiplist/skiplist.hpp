#pragma once

#include <algorithm>
#include <memory>

#include "threading/hazard_ptr.hpp"
#include "threading/sync/lockable.hpp"
#include "threading/sync/spinlock.hpp"

#include "utils/random/fast_binomial.hpp"

// concurrent skiplist based on the implementation described in
// "A Provably Correct Scalable Concurrent Skip List"
// https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf

template <class K, class T, size_t H=32, class lock_t=SpinLock>
class SkipList
{
    static thread_local FastBinomial<H> rnd;

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
        friend class SkipList;

    public:
        class reference
        {
        public:
            reference() {}

            reference(Node* node, bool store_hazard = true)
                : node(node), hazard(store_hazard ? node : nullptr) {}

            reference(const reference&) = delete;

            reference(reference&& other)
                : node(other.node), hazard(std::move(other.hazard))
            {
                other.node = nullptr;
            }

            reference& operator=(const reference& other) = delete;

            reference& operator=(const reference&& other)
            {
                node = other.node;
                other.node = nullptr;
                hazard = std::move(other.hazard);

                return *this;
            }

            T& operator*() { return *node; }
            const T& operator*() const { return *node; }

            T* operator->() { return node; }
            const T* operator->() const { return node; }

            operator T*() { return node; }
            const operator T*() const { return node; }

            const hazard_ptr& get_hazard() const
            {
                return hazard;
            }

        private:
            hazard_ptr hazard;
            Node* node;
        };

        K key;
        T item;

        const uint8_t height;
        Flags flags;

        static Node* create(K key, T item, uint8_t height)
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
            return new (node) Node(key, item, height);
        }

        static void destroy(Node* node)
        {
            node->~Node();
            free(node);
        }

        Node::reference forward(size_t level) const
        {
            while(true)
            {
                auto ref = Node::reference(load_tower(level));

                if(ref.get_hazard() == load_tower(level))
                    return std::move(ref);
            }
        }

    private:
        Node(K key, T item, uint8_t height)
            : key(key), item(item), height(height)
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

        void forward(size_t level, Node* next)
        {
            tower[level].store(next, std::memory_order_release);
        }

        Node* load_tower(size_t level)
        {
            return tower[level].load(std::memory_order_acquire);
        }

        // this creates an array of the size zero. we can't put any sensible
        // value here since we don't know what size it will be untill the
        // node is allocated. we could make it a Node** but then we would
        // have two memory allocations, one for node and one for the forward
        // list. this way we avoid expensive malloc/free calls and also cache
        // thrashing when following a pointer on the heap
        std::atomic<Node*> tower[0];
    };

    using node_ref_t = typename Node::reference;

public:
    class iterator
    {
    public:
        iterator() = default;
        iterator(node_ref_t&& node) : node(std::move(node)) {}
        iterator(const iterator&) = delete;

        T& operator*() { return node; }
        T* operator->() { return node; }

        operator T*() { return node; }

        iterator& operator++()
        {
            node = node->forward(0);
            return *this;
        }

        iterator& operator++(int)
        {
            return operator++();
        }

    private:
        node_ref_t node;
    };

    SkipList() : header(Node::create(K(), T(), H)) {}

    auto begin() { return iterator(header->forward(0)); }
    auto end() { return iterator(); }

    size_t size() const
    {
        return count.load(std::memory_order_acquire);
    }

    iterator find(const K& key)
    {
        auto pred = node_ref_t(header);
        node_ref_t node;

        uint8_t h = pred->height - 1;

        while(true)
        {
            // try to descend down first the next key on this layer overshoots
            for(; h >= 0 && less(key, node = pred->forward(h)); --h) {}

            // if we overshoot at every layer, item doesn't exist
            if(h < 0)
                return iterator();

            // the item is farther to the right, continue going right as long
            // as the key is greater than the current node's key
            while(greater(key, node))
                pred = node, node = node->forward(h);

            // check if we have a hit. if not, we need to descend down again
            if(!less(key, node))
                return iterator(std::move(node));
        }
    }

private:
    std::atomic<size_t> count {0};
    Node* header;

    bool greater(const K& key, const Node* const node)
    {
        return node && key > node->key;
    }

    bool less(const K& key, const Node* const node)
    {
        return (node == nullptr) || key < node->key;
    }

    int find_path(Node& from, int start, const K& key,
                  node_ref_t (preds&)[],
                  node_ref_t (succs&)[])
    {
        int lfound = -1;
        Node* pred = from;

        for(int level = start_level; level >= 0; --level)
        {
            Node* node = pred->forward(level);

            while(greater(key, node))
                pred = node, node = pred->forward(level);


            if(lfound == -1 && !less(key, node))
                lfound = level;

            preds[level] = pred;
            succs[level] = node;
        }

        return lfound;
    }
};
