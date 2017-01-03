#include "gtest/gtest.h"

#include "data_structures/ptr_int.hpp"

TEST(PtrInt, SizeOf)
{
    ASSERT_EQ(sizeof(PtrInt<int *, 1, int>), sizeof(uintptr_t));
}

TEST(PtrInt, ConstructionAndRead)
{
    auto ptr1 = std::make_unique<int>(2);
    PtrInt<int *, 2, int> pack1(ptr1.get(), 1);

    ASSERT_EQ(pack1.get_int(), 1);
    ASSERT_EQ(pack1.get_ptr(), ptr1.get());


    auto ptr2 = std::make_unique<int>(2);
    PtrInt<int *, 3, int> pack2(ptr2.get(), 4);

    ASSERT_EQ(pack2.get_int(), 4);
    ASSERT_EQ(pack2.get_ptr(), ptr2.get());
}

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
