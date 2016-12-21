#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "utils/command_line/arguments.hpp"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wwritable-strings"

TEST_CASE("ProgramArgument FlagOnly Test")
{
    CLEAR_ARGS();

    int argc     = 2;
    char *argv[] = {"ProgramArgument FlagOnly Test", "-test"};

    REGISTER_ARGS(argc, argv);
    REGISTER_REQUIRED_ARGS({"-test"});

    REQUIRE(CONTAINS_FLAG("-test") == true);
}

TEST_CASE("ProgramArgument Single Entry Test")
{
    CLEAR_ARGS();

    int argc     = 3;
    char *argv[] = {"ProgramArgument Single Entry Test", "-bananas", "99"};

    REGISTER_REQUIRED_ARGS({"-bananas"});
    REGISTER_ARGS(argc, argv);

    REQUIRE(GET_ARG("-bananas", "100").get_int() == 99);
}

TEST_CASE("ProgramArgument Multiple Entries Test")
{
    CLEAR_ARGS();

    int argc     = 4;
    char *argv[] = {"ProgramArgument Multiple Entries Test", "-files",
                    "first_file.txt", "second_file.txt"};

    REGISTER_ARGS(argc, argv);

    auto files = GET_ARGS("-files", {});

    REQUIRE(files[0].get_string() == "first_file.txt");
}

TEST_CASE("ProgramArgument Combination Test")
{
    CLEAR_ARGS();

    int argc     = 14;
    char *argv[] = {"ProgramArgument Combination Test",
                    "-run_tests",
                    "-tests",
                    "Test1",
                    "Test2",
                    "Test3",
                    "-run_times",
                    "10",
                    "-export",
                    "test1.txt",
                    "test2.txt",
                    "test3.txt",
                    "-import",
                    "data.txt"};

    REGISTER_ARGS(argc, argv);

    REQUIRE(CONTAINS_FLAG("-run_tests") == true);

    auto tests = GET_ARGS("-tests", {});
    REQUIRE(tests[0].get_string() == "Test1");
    REQUIRE(tests[1].get_string() == "Test2");
    REQUIRE(tests[2].get_string() == "Test3");

    REQUIRE(GET_ARG("-run_times", "0").get_int() == 10);

    auto exports = GET_ARGS("-export", {});
    REQUIRE(exports[0].get_string() == "test1.txt");
    REQUIRE(exports[1].get_string() == "test2.txt");
    REQUIRE(exports[2].get_string() == "test3.txt");

    REQUIRE(GET_ARG("-import", "test.txt").get_string() == "data.txt");
}

#pragma clang diagnostic pop
