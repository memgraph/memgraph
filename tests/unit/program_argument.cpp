#include "gtest/gtest.h"

#include "utils/command_line/arguments.hpp"

// beacuse of c++ 11
// TODO: figure out better solution
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wwritable-strings"

TEST(ProgramArgument, FlagOnly)
{
    CLEAR_ARGS();

    int argc     = 2;
    char *argv[] = {"ProgramArgument FlagOnly Test", "-test"};

    REGISTER_ARGS(argc, argv);
    REGISTER_REQUIRED_ARGS({"-test"});

    ASSERT_EQ(CONTAINS_FLAG("-test"), true);
}

TEST(ProgramArgument, SingleEntry)
{
    CLEAR_ARGS();

    int argc     = 3;
    char *argv[] = {"ProgramArgument Single Entry Test", "-bananas", "99"};

    REGISTER_REQUIRED_ARGS({"-bananas"});
    REGISTER_ARGS(argc, argv);

    ASSERT_EQ(GET_ARG("-bananas", "100").get_int(), 99);
}

TEST(ProgramArgument, MultipleEntries)
{
    CLEAR_ARGS();

    int argc     = 4;
    char *argv[] = {"ProgramArgument Multiple Entries Test", "-files",
                    "first_file.txt", "second_file.txt"};

    REGISTER_ARGS(argc, argv);

    auto files = GET_ARGS("-files", {});

    ASSERT_EQ(files[0].get_string(), "first_file.txt");
}

TEST(ProgramArgument, Combination)
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

    ASSERT_EQ(CONTAINS_FLAG("-run_tests"), true);

    auto tests = GET_ARGS("-tests", {});
    ASSERT_EQ(tests[0].get_string(), "Test1");
    ASSERT_EQ(tests[1].get_string(), "Test2");
    ASSERT_EQ(tests[2].get_string(), "Test3");

    ASSERT_EQ(GET_ARG("-run_times", "0").get_int(), 10);

    auto exports = GET_ARGS("-export", {});
    ASSERT_EQ(exports[0].get_string(), "test1.txt");
    ASSERT_EQ(exports[1].get_string(), "test2.txt");
    ASSERT_EQ(exports[2].get_string(), "test3.txt");

    ASSERT_EQ(GET_ARG("-import", "test.txt").get_string(), "data.txt");
}

#pragma clang diagnostic pop

int main(int argc, char **argv)
{
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
