#ifndef MEMGRAPH_MVCC_VERSION_HPP
#define MEMGRAPH_MVCC_VERSION_HPP

#include <atomic>

namespace mvcc
{

template <class T>
class Version
{
public:
    Version() : versions(nullptr) {}

    ~Version()
    {
        delete versions.load();
    }

    Version(T* value) : versions(value) {}

    // return a pointer to a newer version stored in this record
    T* newer()
    {
        return versions.load();
    }

    // set a newer version of this record
    void newer(T* value)
    {
        versions.store(value);
    }

private:
    // this is an atomic singly-linked list of all versions. this pointer
    // points to a newer version of this record. the newer version also has
    // this pointer which points to an even more recent version. if no newer
    // version is present, this value points to a nullptr
    std::atomic<T*> newer_version;
};

}

#endif
