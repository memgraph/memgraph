#pragma once

#include "utils/option.hpp"

namespace iter
{
template <class U, class I, class MapOperator>
class Map
{

public:
    Map() = delete;
    template <class IT, class OP>
    Map(IT &&iter, OP &&op) : iter(std::move(iter)), op(std::move(op))
    {
    }

    auto next()
    {
        auto item = iter.next();
        if (item.is_present()) {
            return Option<U>(op(item.take()));
        } else {
            return Option<U>();
        }
    }

private:
    I iter;
    MapOperator op;
};

template <class I, class OP>
auto make_map(I &&iter, OP &&op)
{
    return Map<decltype(op(iter.next().take())), I, OP>(std::move(iter),
                                                        std::move(op));
}
}
