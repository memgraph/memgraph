#pragma once

#include <vector>

// TODO: more bytes can be saved if this is array with exact size as number
// of elements.
// TODO: even more bytes can be saved if this is one ptr to structure which
// holds len followed by len sized array.
template <class T>
using ArrayStore = std::vector<T>;
