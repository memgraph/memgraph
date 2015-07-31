#ifndef MEMGRAPH_STORAGE_MODEL_UTILS_VERSION_HPP
#define MEMGRAPH_STORAGE_MODEL_UTILS_VERSION_HPP

#include <atomic>

template <class T>
class Version
{
public:
    Version() : versions(nullptr) {}

    Version(T* value) : versions(value) {}

    // return a pointer to a newer version stored in this record
    T* newer()
    {
        return versions.load(std::memory_order_relaxed);
    }

    // set a newer version of this record
    void newer(T* value)
    {
        versions.store(value, std::memory_order_relaxed);
    }

private:
    // this is an atomic singly-linked list of all versions. this pointer
    // points to a newer version of this record. the newer version also has
    // this pointer which points to an even more recent version. if no newer
    // version is present, this value points to a nullptr
    std::atomic<T*> versions;
};

#endif
