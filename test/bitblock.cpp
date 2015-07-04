#include <thread>

#include "catch.hpp"
#include "data_structures/bitset/bitblock.hpp"
#include "sync/spinlock.hpp"

TEST_CASE("BitBlock should be empty on construction")
{
    BitBlock<> bb;
    REQUIRE(bb.block.load(std::memory_order_relaxed) == 0);
}

template <class block_t, size_t N>
void test_sizes()
{
    BitBlock<block_t, N> bb;
    auto bits = bb.bits;
    auto size = bb.size;

    REQUIRE(bits == (8 * sizeof(block_t)));
    REQUIRE(size == (8 * sizeof(block_t) / N));
}

template <class T, size_t N>
struct test_sizes_loop : test_sizes_loop<T, N - 1>
{
    test_sizes_loop()
    {
        test_sizes<T, N>();
    }
};

template <class T>
struct test_sizes_loop<T, 1>
{
    test_sizes_loop()
    {
        test_sizes<T, 1>();
    }
};

TEST_CASE("BitBlock bit sizes should be set correctly")
{
    test_sizes_loop<uint8_t, 8>  size_test8;
    test_sizes_loop<uint16_t, 8> size_test16;
    test_sizes_loop<uint32_t, 8> size_test32;
    test_sizes_loop<uint64_t, 8> size_test64;
}

TEST_CASE("Values load correctly from the BitBlock")
{
    constexpr uint32_t k = 0xCA3F;

    SECTION("Block size = 1")
    {
        constexpr size_t N = 1;

        BitBlock<uint32_t, N> bb;
        bb.block.store(k);

        for(size_t i = 0; i < bb.size; ++i)
            REQUIRE(bb.at(i) == ((k >> i) & 1));
    }

    SECTION("Block size = 4")
    {
        constexpr size_t N = 4;

        BitBlock<uint32_t, N> bb;
        bb.block.store(k);

        for(size_t i = 0; i < bb.size; ++i)
            REQUIRE(bb.at(i) == ((k >> (N * i)) & 0xF));
    }
}

TEST_CASE("You can set a bit in a BitBlock and get the result")
{
    SECTION("Block size = 1")
    {
        BitBlock<uint8_t, 1> bb;

        for(size_t i = 0; i < bb.bits; ++i)
        {
            bb.set(i, 1);
            
            for(size_t j = 0; j < bb.bits; ++j)
            {
                if(j <= i)
                    REQUIRE(bb.at(j) == 1);
                else
                    REQUIRE(bb.at(j) == 0);
            }
        }
    }

    SECTION("Block size = 2")
    {
        constexpr size_t N = 2;

        BitBlock<uint16_t, N> bb;

        for(size_t i = 0; i < bb.size; ++i)
        {
            auto k = i % (1 << N);
            bb.set(i, k);
            
            for(size_t j = 0; j < bb.size; j++)
            {
                auto l = j % (1 << N);

                if(j <= i)
                    REQUIRE(bb.at(j) == l);
                else
                    REQUIRE(bb.at(j) == 0);
            }
        }
    }

    SECTION("Block size = 5")
    {
        constexpr size_t N = 5;

        BitBlock<uint16_t, N> bb;

        for(size_t i = 0; i < bb.size; ++i)
        {
            auto k = i % (1 << N);
            bb.set(i, k);
            
            for(size_t j = 0; j < bb.size; j++)
            {
                auto l = j % (1 << N);

                if(j <= i)
                    REQUIRE(bb.at(j) == l);
                else
                    REQUIRE(bb.at(j) == 0);
            }
        }
    }
}

template <class T, size_t N>
void bitblock_thead_test(BitBlock<T, N>* bb, size_t idx, size_t n)
{
    static SpinLock lock;
    uint8_t x;

    uint64_t sum = 0, actual = 0;

    for(size_t i = 0; i < n; ++i)
    {
        int y = i % 2;
        actual += i * y;

        bb->set(idx, y);
        x = bb->at(idx);

        sum += i * x;

        bb->clear(idx);
        x = bb->at(idx);
    }

    auto guard = std::unique_lock<SpinLock>(lock);
    REQUIRE(sum == actual);
}

TEST_CASE("(try to) Test multithreaded correctness")
{
    BitBlock<uint64_t, 1> bb;
    
    constexpr int N = 2;
    constexpr int K = 500000;

    std::vector<std::thread> threads;

    for(int i = 0; i < N; ++i)
        threads.push_back(std::thread(
            bitblock_thead_test<uint64_t, 1>, &bb, i, K));

    for(auto& thread : threads){
        thread.join();
    }
}
