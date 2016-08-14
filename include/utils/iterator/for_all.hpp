
#pragma once

#include "utils/option.hpp"

namespace iter
{
template <class I, class C>
void for_all(I &&iter, C &&consumer)
{
    auto e = iter.next();
    while (e.is_present()) {
        consumer(e.take());
        e = iter.next();
    }
}
}
