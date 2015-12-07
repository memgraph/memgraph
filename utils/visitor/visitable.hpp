#pragma once

template <class T>
struct Visitable
{
    virtual ~Visitable() = default;
    virtual void accept(T& visitor) = 0;
};
