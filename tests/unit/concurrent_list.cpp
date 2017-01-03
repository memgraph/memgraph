#include "gtest/gtest.h"

#include "data_structures/concurrent/concurrent_list.hpp"

TEST(ConncurentList, Insert)
{
    ConcurrentList<int> list;
    auto it = list.begin();
    it.push(32);
    it.reset();
    ASSERT_EQ(*it, 32);
}

TEST(ConncurentList, Iterate)
{
    ConcurrentList<int> list;
    auto it = list.begin();
    it.push(32);
    it.push(7);
    it.push(9);
    it.push(0);
    it.reset();

    ASSERT_EQ(*it, 0);
    it++;
    ASSERT_EQ(*it, 9);
    it++;
    ASSERT_EQ(*it, 7);
    it++;
    ASSERT_EQ(*it, 32);
    it++;
    ASSERT_EQ(it, list.end());
}

TEST(ConncurentList, RemoveHead)
{
    ConcurrentList<int> list;
    auto it = list.begin();
    it.push(32);
    it.reset();

    ASSERT_EQ(it.remove(), true);
    ASSERT_EQ(it.is_removed(), true);
    ASSERT_EQ(!it.remove(), true);

    it.reset();
    ASSERT_EQ(it, list.end());
}

TEST(ConncurentList, Remove)
{
    ConcurrentList<int> list;
    auto it = list.begin();
    it.push(32);
    it.push(7);
    it.push(9);
    it.push(0);
    it.reset();

    it++;
    it++;
    ASSERT_EQ(it.remove(), true);
    ASSERT_EQ(it.is_removed(), true);
    ASSERT_EQ(!it.remove(), true);

    it.reset();
    ASSERT_EQ(*it, 0);
    it++;
    ASSERT_EQ(*it, 9);
    it++;
    ASSERT_EQ(*it, 32);
    it++;
    ASSERT_EQ(it, list.end());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
