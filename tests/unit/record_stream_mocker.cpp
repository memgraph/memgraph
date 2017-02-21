#include "gtest/gtest.h"

#include "support/Any.h"

#include "communication/bolt/v1/serialization/record_stream_mocker.hpp"
#include "logging/default.hpp"
#include "logging/streams/stdout.hpp"
#include "utils/command_line/arguments.hpp"

TEST(RecordStreamMocker, Headers) {
  bolt::RecordStreamMocker rs;
  std::vector<std::string> expected_output = {"a", "b", "c", "0"};
  rs.write_fields(expected_output);
  std::vector<std::string> headers = rs.get_message_headers("fields");
  ASSERT_EQ(headers.size(), expected_output.size());
  for (int i = 0; i < expected_output.size(); ++i)
    ASSERT_EQ(headers[i], expected_output[i]);
}

TEST(RecordStreamMocker, OneValue) {
  bolt::RecordStreamMocker rs;
  rs.write_field("n");
  rs.write(TypedValue(5));
  ASSERT_EQ(rs.count_message_columns("fields"), 1);
  std::vector<antlrcpp::Any> output = rs.get_message_column("fields", 0);
  ASSERT_EQ(output.size(), 1);
  auto val = output[0].as<TypedValue>().Value<int>();
  ASSERT_EQ(val, 5);
}

TEST(RecordStreamMocker, OneListOfInts) {
  bolt::RecordStreamMocker rs;
  rs.write_field("n");
  std::vector<int> expected_output = {5, 4, 6, 7};
  rs.write_list_header(expected_output.size());
  for (auto x : expected_output) rs.write(TypedValue(x));
  ASSERT_EQ(rs.count_message_columns("fields"), 1);
  std::vector<antlrcpp::Any> output = rs.get_message_column("fields", 0);
  ASSERT_EQ(output.size(), 1);
  std::vector<antlrcpp::Any> list = output[0].as<std::vector<antlrcpp::Any>>();
  ASSERT_EQ(list.size(), expected_output.size());
  for (int i = 0; i < list.size(); ++i) {
    auto val = list[i].as<TypedValue>().Value<int>();
    ASSERT_EQ(val, expected_output[i]);
  }
}

TEST(RecordStreamMocker, OneListOfIntAndList) {
  bolt::RecordStreamMocker rs;
  rs.write_field("n");
  int expected_value = 42;
  std::vector<int> expected_output = {5, 4, 6, 7};
  rs.write_list_header(2);
  rs.write(expected_value);
  rs.write_list_header(4);
  for (auto x : expected_output) rs.write(TypedValue(x));

  ASSERT_EQ(rs.count_message_columns("fields"), 1);
  std::vector<antlrcpp::Any> output = rs.get_message_column("fields", 0);
  ASSERT_EQ(output.size(), 1);
  std::vector<antlrcpp::Any> list = output[0].as<std::vector<antlrcpp::Any>>();
  ASSERT_EQ(list.size(), 2);
  ASSERT_EQ(list[0].as<TypedValue>().Value<int>(), expected_value);

  auto list_inside = list[1].as<std::vector<antlrcpp::Any>>();
  for (int i = 0; i < list_inside.size(); ++i) {
    auto val = list_inside[i].as<TypedValue>().Value<int>();
    ASSERT_EQ(val, expected_output[i]);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());
  return RUN_ALL_TESTS();
}
