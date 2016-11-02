#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "template_engine/engine.hpp"

TEST_CASE("Template Engine - basic placeholder replacement")
{
    auto rendered = template_engine::render("{{one}} {{two}}",
                                            {{"one", "two"}, {"two", "one"}});

    REQUIRE(rendered == "two one");
}
