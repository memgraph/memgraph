#pragma once

template <class T>
class OptionPtr
{
public:
    OptionPtr() {}
    OptionPtr(T *ptr) : ptr(ptr) {}

    bool is_present() { return ptr != nullptr; }

    T *get()
    {
        assert(is_present());
        return ptr;
    }

private:
    T *ptr = nullptr;
};
