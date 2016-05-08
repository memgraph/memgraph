#include <iostream>
#include <mutex>
#include <vector>
#include <chrono>

#include "threading/thread.hpp"
#include "threading/hazard_ptr.hpp"

std::mutex mutex;

struct Foo
{
    int bar = 0;
};

void scan_foos(const std::vector<Foo>& foos)
{
    auto& hp = HazardStore::get();

    std::unique_lock<std::mutex> cout_guard(mutex);
    std::cout << "Scanning foos..." << std::endl;

    for(auto& foo : foos)
    {
        auto foo_ptr = &foo;

        std::unique_lock<std::mutex> cout_guard(mutex);
        std::cout << "Foo taken? " << hp.scan(foo_ptr) << std::endl;
    }
}

int main(void)
{
    using namespace std::chrono_literals;
    std::cout << std::boolalpha;

    static constexpr size_t NUM_THREADS = 8;

    std::vector<Thread> threads;

    std::vector<Foo> foos;
    foos.resize(NUM_THREADS + 2);

    for(size_t i = 0; i < NUM_THREADS; ++i)
        threads.emplace_back([&foos]() {
            auto id = this_thread::id;

            auto foo = &foos.at(id);
            auto hazard = hazard_ptr(foo);

            foo->bar = id;

            std::unique_lock<std::mutex> cout_guard(mutex);
            std::cout << "Hello from thread " << this_thread::id << std::endl;

            std::this_thread::sleep_for(5s);
        });

    // 0 to NUM_THREADS foos should be taken
    // maybe none, maybe all!
    scan_foos(foos);

    std::this_thread::sleep_for(3s);

    // first NUM_THREADS foos should be taken
    scan_foos(foos);

    std::this_thread::sleep_for(3s);

    // all foos should be available now
    scan_foos(foos);

    for(auto& thread : threads)
        thread.join();

    return 0;
}
