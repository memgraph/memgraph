#include "gtest/gtest.h"

#include "query/procedure/mg_procedure_impl.hpp"
#include "query/procedure/module.hpp"
#include "test_utils.hpp"

TEST(MgpTransTest, TestMgpTransApi) {
  constexpr auto no_op_cb = [](mgp_messages *msg, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {};
  mgp_module module(utils::NewDeleteResource());
  // If this is false, then mgp_module_add_transformation()
  // correctly calls IsValidIdentifier(). We don't need to test
  // for different string cases as these are all handled by
  // IsValidIdentifier().
  // Maybe add a mock instead and expect IsValidIdentifier() to be called once?
  EXPECT_EQ(mgp_module_add_transformation(&module, "dash-dash", no_op_cb), MGP_ERROR_INVALID_ARGUMENT);
  EXPECT_TRUE(module.transformations.empty());

  EXPECT_EQ(mgp_module_add_transformation(&module, "transform", no_op_cb), MGP_ERROR_NO_ERROR);
  EXPECT_NE(module.transformations.find("transform"), module.transformations.end());

  // Try to register a transformation twice
  EXPECT_EQ(mgp_module_add_transformation(&module, "transform", no_op_cb), MGP_ERROR_LOGIC_ERROR);
  EXPECT_TRUE(module.transformations.size() == 1);
}
