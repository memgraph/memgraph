#include "utils/iterator/iterator.hpp"

template <class T>
template <class OP>
auto IteratorBase<T>::map(OP &&op)
{
    return iter::make_map<decltype(std::move(*this)), OP>(std::move(*this),
                                                          std::move(op));
}

template <class T>
template <class OP>
auto IteratorBase<T>::filter(OP &&op)
{
    return iter::make_filter<decltype(std::move(*this)), OP>(std::move(*this),
                                                             std::move(op));
}

template <class T>
auto IteratorBase<T>::to()
{
    return map([](auto er) { return er.to(); }).fill();
}

template <class T>
auto IteratorBase<T>::fill()
{
    return filter([](auto &ra) { return ra.fill(); });
}

template <class T>
template <class TYPE>
auto IteratorBase<T>::type(TYPE const &type)
{
    return filter([&](auto &ra) { return ra.edge_type() == type; });
}

template <class T>
template <class OP>
void IteratorBase<T>::for_all(OP &&op)
{
    iter::for_all(std::move(*this), std::move(op));
}
