#ifndef MEMGRAPH_UTILS_VISITOR_VISITABLE_HPP
#define MEMGRAPH_UTILS_VISITOR_VISITABLE_HPP

template <class T>
struct Visitable
{
    virtual ~Visitable() {}
    virtual void accept(T& visitor) = 0;
};

#endif
