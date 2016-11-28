#define CATCH_CONFIG_MAIN
#include "catch.hpp"

#include "utils/command_line/arguments.hpp"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wwritable-strings"

TEST_CASE("ProgramArgument FlagOnly Test") {
  ProgramArguments::instance().clear();

  int argc = 2;

  char* argv[] = {"ProgramArgument FlagOnly Test", "-test"};

  ProgramArguments::instance().register_args(argc, argv);
  ProgramArguments::instance().register_required_args({"-test"});

  REQUIRE(ProgramArguments::instance().contains_flag("-test") == true);
}

TEST_CASE("ProgramArgument Single Entry Test") {
  ProgramArguments::instance().clear();

  int argc = 3;

  char* argv[] = {"ProgramArgument Single Entry Test", "-bananas", "99"};

  ProgramArguments::instance().register_required_args({"-bananas"});
  ProgramArguments::instance().register_args(argc, argv);

  REQUIRE(
      ProgramArguments::instance().get_arg("-bananas", "100").GetInteger() ==
      99);
}

TEST_CASE("ProgramArgument Multiple Entries Test") {
  ProgramArguments::instance().clear();

  int argc = 4;

  char* argv[] = {"ProgramArgument Multiple Entries Test", "-files",
                  "first_file.txt", "second_file.txt"};

  ProgramArguments::instance().register_args(argc, argv);

  auto files = ProgramArguments::instance().get_arg_list("-files", {});

  REQUIRE(files[0].GetString() == "first_file.txt");
}

TEST_CASE("ProgramArgument Combination Test") {
  ProgramArguments::instance().clear();

  int argc = 14;

  char* argv[] = {"ProgramArgument Combination Test",
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

  ProgramArguments::instance().register_args(argc, argv);

  REQUIRE(ProgramArguments::instance().contains_flag("-run_tests") == true);

  auto tests = ProgramArguments::instance().get_arg_list("-tests", {});
  REQUIRE(tests[0].GetString() == "Test1");
  REQUIRE(tests[1].GetString() == "Test2");
  REQUIRE(tests[2].GetString() == "Test3");

  REQUIRE(
      ProgramArguments::instance().get_arg("-run_times", "0").GetInteger() ==
      10);

  auto exports = ProgramArguments::instance().get_arg_list("-export", {});
  REQUIRE(exports[0].GetString() == "test1.txt");
  REQUIRE(exports[1].GetString() == "test2.txt");
  REQUIRE(exports[2].GetString() == "test3.txt");

  REQUIRE(
      ProgramArguments::instance().get_arg("-import", "test.txt").GetString() ==
      "data.txt");
}

#pragma clang diagnostic pop
