#ifndef MEMGRAPH_DATA_STRUCTURES_UNION_FIND_UNION_FIND_HPP
#define MEMGRAPH_DATA_STRUCTURES_UNION_FIND_UNION_FIND_HPP

#include <memory>

template <class uintXX_t = uint32_t,
          class allocator = std::allocator<uintXX_t>>
class UnionFind
{
public:
    UnionFind(uintXX_t n) : N(n), n(n)
    {
        count = alloc.allocate(n);
        parent = alloc.allocate(n);

        for(auto i = 0; i < n; ++i)
            count[i] = 1, parent[i] = i;
    }

    ~UnionFind()
    {
        alloc.deallocate(count, N);
        alloc.deallocate(parent, N);
    }

    // this is O(lg* n)
    void connect(uintXX_t p, uintXX_t q)
    {
        auto rp = root(p);
        auto rq = root(q);

        // if roots are equal, we don't have to do anything
        if(rp == rq)
            return;

        // merge the smaller subtree to the root of the larger subtree
        if(count[rp] < count[rq])
            parent[rp] = rq, count[rp] += count[rp];
        else
            parent[rq] = rp, count[rp] += count[rq];

        // update the number of groups
        n--;
    }

    // O(lg* n)
    bool find(uintXX_t p, uintXX_t q)
    {
        return root(p) == root(q);
    }

    // O(lg* n)
    uintXX_t root(uintXX_t p)
    {
        auto r = p;
        auto newp = p;

        // find the node connected to itself, that's the root
        while(parent[r] != r)
            r = parent[r];

        // do some path compression to enable faster searches
        while(p != r)
            newp = parent[p], parent[p] = r, p = newp;

        return r;
    }

    uintXX_t size() const
    {
        return n;
    }

private:
    allocator alloc;

    const uintXX_t N;
    uintXX_t n;

    // array of subtree counts
    uintXX_t* count;

    // array of tree indices
    uintXX_t* parent;
};

#endif
