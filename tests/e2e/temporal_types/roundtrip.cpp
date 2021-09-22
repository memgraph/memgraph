#include "fmt/format.h"
#include "mgclient.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

struct DurationParams {
  double days{0};
  double hours{0};
  double minutes{0};
  double seconds{0};
  double subseconds{0};
};

void RoundtripDuration(mg::Client &client, const std::string_view group, const std::string_view property,
                       DurationParams params) {
  const auto dur_str =
      fmt::format("P{}DT{}H{}M{}.{}S", params.days, params.hours, params.minutes, params.seconds, params.subseconds);
  const auto query = fmt::format("CREATE (:{} {{{}: DURATION(\"{}\")}})", group, property, dur_str);
  auto executed = client.Execute(query);
  MG_ASSERT(executed, "Failed to execute duration query");
  client.DiscardAll();
  executed = client.Execute(fmt::format("MATCH (u:{}) return u", group));
  MG_ASSERT(executed, "Failed to execute query");
  const auto result = client.FetchAll();
  MG_ASSERT(result, "Failed to fetch results");
  MG_ASSERT(result->size() == 1, "Failed to fetch correct result size");
  const auto node = (*result)[0][0].ValueNode();
  const auto it = node.properties().find(property);
  MG_ASSERT(it != node.properties().end(), fmt::format("Failed to find property {}", property));
  const auto dur = (*it).second.ValueDuration();
  const auto expected = utils::Duration(utils::Duration(utils::ParseDurationParameters(dur_str)));
  MG_ASSERT(dur.months() == 0, "Received incorrect months in the duration");
  MG_ASSERT(dur.days() == expected.Days(), "Received incorrect days in the duration");
  MG_ASSERT(dur.seconds() == expected.SubDaysAsSeconds(), "Received incorrect seconds in the duration");
  MG_ASSERT(dur.nanoseconds() == expected.SubSecondsAsNanoseconds(), "Received incorrect nanoseconds in the duration");
}

void RoundtripDate(mg::Client &client, const std::string_view group, const std::string_view property,
                   utils::DateParameters params) {
  const auto date_str = fmt::format("{:0>2}-{:0>2}-{:0>2}", params.years, params.months, params.days);
  const auto query = fmt::format("CREATE (:{} {{{}: DATE(\"{}\")}})", group, property, date_str);
  auto executed = client.Execute(query);
  MG_ASSERT(executed, "Failed to execute duration query");
  client.DiscardAll();
  executed = client.Execute(fmt::format("MATCH (u:{}) return u", group));
  MG_ASSERT(executed, "Failed to execute query");
  const auto result = client.FetchAll();
  MG_ASSERT(result, "Failed to fetch results");
  MG_ASSERT(result->size() == 1, "Failed to fetch correct result size");
  const auto node = (*result)[0][0].ValueNode();
  const auto it = node.properties().find(property);
  MG_ASSERT(it != node.properties().end(), fmt::format("Failed to find property {}", property));
  const auto date = (*it).second.ValueDate();
  const auto expected = utils::Date(params);
  MG_ASSERT(date.days() == expected.DaysSinceEpoch(), "Received incorrect days in the date roundtrip");
}

struct LocalTimeParams {
  int64_t hours{0};
  int64_t minutes{0};
  int64_t seconds{0};
  int64_t subseconds{0};
};

void RoundtripLocalTime(mg::Client &client, const std::string_view group, const std::string_view property,
                        LocalTimeParams params) {
  const auto lt_str =
      fmt::format("{:0>2}:{:0>2}:{:0>2}.{:0>6}", params.hours, params.minutes, params.seconds, params.subseconds);
  const auto query = fmt::format("CREATE (:{} {{{}: LOCALTIME(\"{}\")}})", group, property, lt_str);
  auto executed = client.Execute(query);
  MG_ASSERT(executed, "Failed to execute duration query");
  client.DiscardAll();
  executed = client.Execute(fmt::format("MATCH (u:{}) return u", group));
  MG_ASSERT(executed, "Failed to execute query");
  const auto result = client.FetchAll();
  MG_ASSERT(result, "Failed to fetch results");
  MG_ASSERT(result->size() == 1, "Failed to fetch correct result size");
  const auto node = (*result)[0][0].ValueNode();
  const auto it = node.properties().find(property);
  MG_ASSERT(it != node.properties().end(), fmt::format("Failed to find property {}", property));
  const auto lt = (*it).second.ValueLocalTime();
  const auto expected = utils::LocalTime(utils::ParseLocalTimeParameters(lt_str).first);
  MG_ASSERT(lt.nanoseconds() == expected.NanosecondsSinceEpoch(),
            "Received incorrect nanoseconds in the LocalTime roundtrip");
}

void RoundtripLocalDateTime(mg::Client &client, const std::string_view group, const std::string_view property,
                            utils::DateParameters d_params, LocalTimeParams lt_params) {
  const auto date_str = fmt::format("{:0>2}-{:0>2}-{:0>2}", d_params.years, d_params.months, d_params.days);
  const auto lt_str = fmt::format("{:0>2}:{:0>2}:{:0>2}", lt_params.hours, lt_params.minutes, lt_params.seconds);
  const auto ldt_str = date_str + "T" + lt_str;
  const auto query = fmt::format("CREATE (:{} {{{}: LOCALDATETIME(\"{}\")}})", group, property, ldt_str);
  auto executed = client.Execute(query);
  MG_ASSERT(executed, "Failed to execute duration query");
  client.DiscardAll();
  executed = client.Execute(fmt::format("MATCH (u:{}) return u", group));
  MG_ASSERT(executed, "Failed to execute query");
  const auto result = client.FetchAll();
  MG_ASSERT(result, "Failed to fetch results");
  MG_ASSERT(result->size() >= 1, "Failed to fetch correct result size");
  const auto node = (*result)[0][0].ValueNode();
  const auto it = node.properties().find(property);
  MG_ASSERT(it != node.properties().end(), fmt::format("Failed to find property {}", property));
  const auto ldt = (*it).second.ValueLocalDateTime();
  const auto [dt, lt] = utils::ParseLocalDateTimeParameters(ldt_str);
  const auto expected = utils::LocalDateTime(dt, lt);
  MG_ASSERT(ldt.seconds() == expected.SecondsSinceEpoch(), "Received incorrect seconds in the LocalDateTime roundtrip");
  MG_ASSERT(ldt.nanoseconds() == expected.SubSecondsAsNanoseconds(),
            "Received incorrect nanoseconds in the LocalDateTime roundtrip");
}

void TestDate(mg::Client &client) {
  RoundtripDate(client, "Person1", "dob", {1960, 1, 12});
  RoundtripDate(client, "Person2", "dob", {1970, 1, 1});
  RoundtripDate(client, "Person3", "dob", {1971, 2, 2});
  RoundtripDate(client, "Person4", "dob", {1991, 7, 29});
  RoundtripDate(client, "Person5", "dob", {1998, 9, 9});
}

void TestLocalTime(mg::Client &client) {
  RoundtripLocalTime(client, "LT1", "time", {1, 3, 3, 33});
  RoundtripLocalTime(client, "LT2", "time", {13, 4, 44, 1002});
  RoundtripLocalTime(client, "LT3", "time", {18, 22, 21, 68010});
  RoundtripLocalTime(client, "LT4", "time", {19, 31, 0, 0});
  RoundtripLocalTime(client, "LT5", "time", {});
}

void TestLocalDateTime(mg::Client &client) {
  RoundtripLocalDateTime(client, "LDT1", "time", {1200, 8, 9}, {12, 33, 1});
  RoundtripLocalDateTime(client, "LDT2", "time", {1961, 6, 3}, {11, 22, 10});
  RoundtripLocalDateTime(client, "LDT3", "time", {1971, 1, 1}, {15, 16, 2});
  RoundtripLocalDateTime(client, "LDT4", "time", {2000, 1, 1}, {2, 33, 1});
  RoundtripLocalDateTime(client, "LDT5", "time", {2021, 9, 21}, {16, 57, 1});
}

void TestDuration(mg::Client &client) {
  RoundtripDuration(client, "Runner1", "time", {3, 5, 6, 2, 1});
  RoundtripDuration(client, "Runner2", "time", {8, 9, 8, 4, 12});
  RoundtripDuration(client, "Runner3", "time", {10, 10, 12, 44, 222222});
  RoundtripDuration(client, "Runner4", "time", {23, 11, 13, 59, 131459});
  RoundtripDuration(client, "Runner5", "time", {0, 110, 14, 88, 131459});
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E temporal types roundtrip");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  logging::RedirectToStderr();

  mg::Client::Init();
  auto client = mg::Client::Connect({});
  MG_ASSERT(client, "Failed to connect with memgraph");
  TestDate(*client);
  TestLocalTime(*client);
  TestLocalDateTime(*client);
  TestDuration(*client);
  return 0;
}
