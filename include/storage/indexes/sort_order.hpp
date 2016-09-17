#pragma once

// TODO: DEPRICATED

template <class T>
struct Ascending
{
    constexpr bool operator()(const T &lhs, const T &rhs) const
    {
        return lhs < rhs;
    }
};

template <class T>
struct Descending
{
    constexpr bool operator()(const T &lhs, const T &rhs) const
    {
        return lhs > rhs;
    }
};
