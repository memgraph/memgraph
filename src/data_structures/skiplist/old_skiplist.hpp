#pragma once

#include <algorithm>
#include <cstdlib>
#include <array>

#include "new_height.hpp"
#include "skipnode.hpp"

// concurrent skiplist based on the implementation described in
// "A Provably Correct Scalable Concurrent Skip List"
// https://www.cs.tau.ac.il/~shanir/nir-pubs-web/Papers/OPODIS2006-BA.pdf

template <class K,
          class T,
          size_t MAX_HEIGHT = 24,
          class compare=std::less<K>,
          class lock_type=SpinLock>
class SkipList
{
    using Node = SkipNode<K, T, lock_type>;

public:
    SkipList()
        : size_(0),
          header(Node::create(MAX_HEIGHT, nullptr, nullptr)) {}

    ~SkipList()
    {
        for(Node* current = header.load(); current;)
        {
            Node* next = current->forward(0);
            Node::destroy(current);
            current = next;
        }
    }

    size_t size() const
    {
        return size_.load();
    }

    uint8_t height() const
    {
        return MAX_HEIGHT;
    }

//private:

    bool greater(const K* const key, const Node* node)
    {
        return node && compare()(*node->key, *key);
    }

    bool less(const K* const key, const Node* node)
    {
        return (node == nullptr) || compare()(*key, *node->key);
    }

    size_t increment_size(size_t delta)
    {
        return size_.fetch_add(delta) + delta;
    }

    int find_path(Node* from,
                  int start_level,
                  const K* const key,
                  Node* preds[],
                  Node* succs[])
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

    Node* find(const K* const key)
    {
        Node* pred = header.load(); 
        Node* node = nullptr;

        uint8_t level = pred->height;
        bool found = false;

        while(!found)
        {
            // descend down first, facebook says it works better xD but make
            // some tests when you have time to determine the best strategy
            for(; level > 0 &&
                  less(key, node = pred->forward(level - 1)); --level) {}

            if(level == 0)
                return nullptr;

            --level;

            while(greater(key, node))
                pred = node, node = node->forward(level);

            found = !less(key, node);
        }

        return node;
    }

    template <bool ADDING>
    bool lock_nodes(uint8_t height,
                    std::unique_lock<lock_type> guards[MAX_HEIGHT],
                    Node* preds[MAX_HEIGHT],
                    Node* succs[MAX_HEIGHT])
    {
        Node *prepred, *pred, *succ = nullptr;
        bool valid = true;

        for(int level = 0; valid && level < height; ++level)
        {
            pred = preds[level], succ = succs[level];

            if(pred != prepred)
                guards[level] = pred->guard(), prepred = pred;

            valid = !pred->marked() && pred->forward(level) == succ;

            if(ADDING)
                valid = valid && (succ == nullptr || !succ->marked());
        }

        return valid;
    }

    bool insert(K* key, T* item)
    {
        Node *preds[MAX_HEIGHT], *succs[MAX_HEIGHT];

        while(true)
        {
            auto head = header.load();
            auto lfound = find_path(head, MAX_HEIGHT - 1, key, preds, succs);

            if(lfound != -1)
            {
                auto found = succs[lfound];
                
                if(!found->marked())
                {
                    while(!found->fully_linked()) {}
                    return false;
                }

                continue;
            }

            auto node_height = new_height(MAX_HEIGHT);
            std::unique_lock<lock_type> guards[MAX_HEIGHT];

            // try to acquire the locks for predecessors up to the height of
            // the new node. release the locks and try again if someone else
            // has the locks
            if(!lock_nodes<true>(node_height, guards, preds, succs))
                continue;

            // you have the locks, create a new node
            auto new_node = Node::create(node_height, key, item);

            // link the predecessors and successors, e.g.
            //
            // 4 HEAD ... P ------------------------> S ... NULL
            // 3 HEAD ... ... P -----> NEW ---------> S ... NULL
            // 2 HEAD ... ... P -----> NEW -----> S ... ... NULL
            // 1 HEAD ... ... ... P -> NEW -> S ... ... ... NULL
            for(uint8_t level = 0; level < node_height; ++level)
            {
                new_node->forward(level, succs[level]);
                preds[level]->forward(level, new_node);
            }

            new_node->set_fully_linked();
            increment_size(1);
            
            return true;
        }
    }

    bool ok_delete(Node* node, int level)
    {
        return node->fully_linked()
            && node->height - 1 == level
            && !node->marked();
    }

    bool remove(const K* const key)
    {
        Node* node = nullptr;
        std::unique_lock<lock_type> node_guard;
        bool marked = false;
        int node_height = 0;

        Node* preds[MAX_HEIGHT], *succs[MAX_HEIGHT];
    
        while(true)
        {
            auto head = header.load();
            auto lfound = find_path(head, MAX_HEIGHT - 1, key, preds, succs);

            if(!marked && (lfound == -1 || !ok_delete(succs[lfound], lfound)))
                    return false;

            if(!marked)
            {
                node = succs[lfound];
                node_height = node->height;
                node_guard = node->guard();

                if(node->marked())
                    return false;

                node->set_marked();
            }

            std::unique_lock<lock_type> guards[MAX_HEIGHT];

            if(!lock_nodes<false>(node_height, guards, preds, succs))
                continue;

            for(int level = node_height - 1; level >= 0; --level)
                preds[level]->forward(level, node->forward(level));

            increment_size(-1);
            break;
        }

        // TODO recyclee(node);
        return true;
    }

    std::atomic<size_t> size_;
    std::atomic<Node*> header;
};
