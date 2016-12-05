#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "data_structures/ptr_int.hpp"

TEST_CASE("Size of pointer integer object")
{
    REQUIRE(sizeof(PtrInt<int *, 1, int>) == sizeof(uintptr_t));
}

TEST_CASE("Construct and read pointer integer pair type")
{
    auto ptr1 = std::make_unique<int>(2);
    PtrInt<int *, 2, int> pack1(ptr1.get(), 1);

    REQUIRE(pack1.get_int() == 1);
    REQUIRE(pack1.get_ptr() == ptr1.get());


    auto ptr2 = std::make_unique<int>(2);
    PtrInt<int *, 3, int> pack2(ptr2.get(), 4);

    REQUIRE(pack2.get_int() == 4);
    REQUIRE(pack2.get_ptr() == ptr2.get());
}
