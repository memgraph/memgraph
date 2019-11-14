#include <gtest/gtest.h>

#include "query/procedure/mg_procedure_impl.hpp"

TEST(CypherType, PresentableNameSimpleTypes) {
  EXPECT_EQ(mgp_type_any()->impl->GetPresentableName(), "ANY");
  EXPECT_EQ(mgp_type_bool()->impl->GetPresentableName(), "BOOLEAN");
  EXPECT_EQ(mgp_type_string()->impl->GetPresentableName(), "STRING");
  EXPECT_EQ(mgp_type_int()->impl->GetPresentableName(), "INTEGER");
  EXPECT_EQ(mgp_type_float()->impl->GetPresentableName(), "FLOAT");
  EXPECT_EQ(mgp_type_number()->impl->GetPresentableName(), "NUMBER");
  EXPECT_EQ(mgp_type_map()->impl->GetPresentableName(), "MAP");
  EXPECT_EQ(mgp_type_node()->impl->GetPresentableName(), "NODE");
  EXPECT_EQ(mgp_type_relationship()->impl->GetPresentableName(),
            "RELATIONSHIP");
  EXPECT_EQ(mgp_type_path()->impl->GetPresentableName(), "PATH");
}

TEST(CypherType, PresentableNameCompositeTypes) {
  {
    const auto *nullable_any = mgp_type_nullable(mgp_type_any());
    EXPECT_EQ(nullable_any->impl->GetPresentableName(), "ANY?");
  }
  {
    const auto *nullable_any =
        mgp_type_nullable(mgp_type_nullable(mgp_type_any()));
    EXPECT_EQ(nullable_any->impl->GetPresentableName(), "ANY?");
  }
  {
    const auto *nullable_list =
        mgp_type_nullable(mgp_type_list(mgp_type_any()));
    EXPECT_EQ(nullable_list->impl->GetPresentableName(), "LIST? OF ANY");
  }
  {
    const auto *list_of_int = mgp_type_list(mgp_type_int());
    EXPECT_EQ(list_of_int->impl->GetPresentableName(), "LIST OF INTEGER");
  }
  {
    const auto *list_of_nullable_path =
        mgp_type_list(mgp_type_nullable(mgp_type_path()));
    EXPECT_EQ(list_of_nullable_path->impl->GetPresentableName(),
              "LIST OF PATH?");
  }
  {
    const auto *list_of_list_of_map =
        mgp_type_list(mgp_type_list(mgp_type_map()));
    EXPECT_EQ(list_of_list_of_map->impl->GetPresentableName(),
              "LIST OF LIST OF MAP");
  }
  {
    const auto *nullable_list_of_nullable_list_of_nullable_string =
        mgp_type_nullable(mgp_type_list(mgp_type_nullable(
            mgp_type_list(mgp_type_nullable(mgp_type_string())))));
    EXPECT_EQ(nullable_list_of_nullable_list_of_nullable_string->impl
                  ->GetPresentableName(),
              "LIST? OF LIST? OF STRING?");
  }
}
