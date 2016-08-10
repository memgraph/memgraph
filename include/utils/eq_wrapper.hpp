#pragma once

template <class T>
class EqWrapper
{
public:
    EqWrapper(T t) : t(t) {}

    friend bool operator==(const EqWrapper &a, const EqWrapper &b)
    {
        return a.t == b.t;
    }

    friend bool operator!=(const EqWrapper &a, const EqWrapper &b)
    {
        return !(a == b);
    }

    T t;
};
