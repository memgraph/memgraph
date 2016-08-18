#pragma once

#include "utils/option.hpp"

// Defines Including as [ and Excluding < for ranges.
enum BorderType
{
    Including = 0,
    Excluding = 1,
};

template <class T>
class Border
{

public:
    Border() : key(Option<T>()), type(Including) {}
    Border(T Tey, BorderType type) : key(Option<T>(std::move(key))), type(type)
    {
    }

    // Border(Border &other) = default;
    Border(Border &other) = default;
    Border(Border &&other) = default;

    Border &operator=(Border &&other) = default;
    Border &operator=(Border &other) = default;

    // true if no border or this>key or this>=key depends on border type.
    bool operator>(const T &other) const
    {
        return !key.is_present() || key.get() > other ||
               (type == Including && key.get() == other);
    }

    // true if no border or this<key or this<=key depends on border type.
    bool operator<(const T &other) const
    {
        return !key.is_present() || key.get() < other ||
               (type == Including && key.get() == other);
    }

    Option<T> key;
    const BorderType type;
};

template <class T>
auto make_inf_border()
{
    return Border<T>();
}
