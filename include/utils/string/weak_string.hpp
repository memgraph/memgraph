#pragma once

#include <cstring>
#include <cassert>
#include <string>

#include "utils/total_ordering.hpp"
#include "utils/total_ordering_with.hpp"

class WeakString
{
public:
    constexpr WeakString() : str(nullptr), len(0) {}

    WeakString(const std::string& str) : str(str.c_str()), len(str.size()) {}

    WeakString(const char* str) : str(str), len(strlen(str)) {}

    constexpr WeakString(const char* str, size_t len) : str(str), len(len) {}

    const char& operator[](size_t idx) const
    {
        assert(idx < len);
        return str[idx];
    }

    const char& front() const
    {
        assert(len > 0);
        return str[0];
    }

    const char& back() const
    {
        assert(len > 0);
        return str[len - 1];
    }

    const char* data() const
    {
        return str;
    }

    bool empty() const
    {
        return len == 0;
    }

    size_t size() const
    {
        return len;
    }

    size_t length() const
    {
        return size();
    }

    std::string to_string() const
    {
        return std::string(str, len);
    }

    friend bool operator==(const WeakString& lhs, const WeakString rhs)
    {
        // oh dear god, make this better with custom iterators
        if(lhs.size() != rhs.size())
            return false;

        for(size_t i = 0; i < lhs.size(); ++i)
            if(lhs[i] != rhs[i])
                return false;

        return true;
    }

    friend bool operator!=(const WeakString& lhs, const WeakString& rhs)
    {
        return !(lhs == rhs);
    }

private:
    const char* str;
    size_t len;
};
