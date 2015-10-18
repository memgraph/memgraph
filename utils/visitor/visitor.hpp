#ifndef MEMGRAPH_UTILS_VISITOR_VISITOR_HPP
#define MEMGRAPH_UTILS_VISITOR_VISITOR_HPP

namespace detail
{

template <typename T>
struct VisitorBase {
    virtual ~VisitorBase() = default;

    virtual void visit(T&) {}
    virtual void post_visit(T&) {}
};

template<typename... T>
struct RecursiveVisitorBase;

template <typename Head, typename... Tail>
struct RecursiveVisitorBase<Head, Tail...>
    : VisitorBase<Head>, RecursiveVisitorBase<Tail...>
{
    using VisitorBase<Head>::visit;
    using VisitorBase<Head>::post_visit;

    using RecursiveVisitorBase<Tail...>::visit;
    using RecursiveVisitorBase<Tail...>::post_visit;
};

template<typename T>
struct RecursiveVisitorBase<T> : public VisitorBase<T>
{
    using VisitorBase<T>::visit;
    using VisitorBase<T>::post_visit;
};

}

template <typename... T>
struct Visitor : public detail::RecursiveVisitorBase<T...>
{
    using detail::RecursiveVisitorBase<T...>::visit;
    using detail::RecursiveVisitorBase<T...>::post_visit;
};

#endif
