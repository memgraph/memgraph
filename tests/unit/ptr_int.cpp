#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "data_structures/ptr_int.hpp"

TEST_CASE("Construct and read pointer integer pair type")
{
    auto ptr1 = std::make_unique<int>(2);
    PtrInt<int *, 2, int> pack1(ptr1.get(), 1);

    REQUIRE(pack1.get_int() == 1);
    REQUIRE(pack1.get_ptr() == ptr1.get());


    auto ptr2 = std::make_unique<int>(2);
    PtrInt<int *, 3, int> pack2(ptr2.get(), 2);

    REQUIRE(pack2.get_int() == 2);
    REQUIRE(pack2.get_ptr() == ptr2.get());
}
