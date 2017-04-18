#include "gtest/gtest.h"

#include "template_engine/engine.hpp"

TEST(TemplateEngine, BasicPlaceholderReplacement) {
  auto rendered = template_engine::Render("{{one}} {{two}}",
                                          {{"one", "two"}, {"two", "one"}});

  ASSERT_EQ(rendered, "two one");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
