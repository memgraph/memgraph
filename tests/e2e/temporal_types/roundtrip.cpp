#include <variant>

#include <gflags/gflags.h>
#include "fmt/format.h"
#include "mgclient.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

struct DurationParams {
  int64_t days{0};
  int64_t hours{0};
  int64_t minutes{0};
  int64_t seconds{0};
  int64_t subseconds{0};
};

struct DurationParamsDays {
  double days{0};
};

struct DurationParamsHours {
  double hours{0};
};

struct DurationParamsMinutes {
  double minutes{0};
};

struct DurationParamsSeconds {
  double seconds{0};
};

void MaybeExecuteQuery(mg::Client &client, const std::string &query, const std::string_view name) {
  auto executed = client.Execute(query);
  MG_ASSERT(executed, fmt::format("Failed to execute {} query", name));
  client.DiscardAll();
}

auto MaybeExecuteMatch(mg::Client &client, const std::string_view group) {
  auto executed = client.Execute(fmt::format("MATCH (u:{}) return u", group));
  MG_ASSERT(executed, "Failed to execute query");
  const auto result = client.FetchAll();
  MG_ASSERT(result, "Failed to fetch results");
  MG_ASSERT(result->size() == 1, "Failed to fetch correct result size");
  return result;
}

auto GetItFromNodeProperty(const mg::ConstMap &props, const std::string_view property) {
  const auto it = props.find(property);
  MG_ASSERT(it != props.end(), fmt::format("Failed to find property {}", property));
  return it;
}

template <typename... Overloads>
struct ScopedOverloads : Overloads... {
  ScopedOverloads(Overloads... ov) : Overloads(ov)... {};
  using Overloads::operator()...;
};

using DurationParameters =
    std::variant<DurationParams, DurationParamsDays, DurationParamsHours, DurationParamsMinutes, DurationParamsSeconds>;
void RoundtripDuration(mg::Client &client, const std::string_view group, const std::string_view property,
                       DurationParameters params) {
  const auto dur_str =
      std::visit(ScopedOverloads(
                     [](const DurationParams &p) {
                       return fmt::format("P{}DT{}H{}M{}.{}S", p.days, p.hours, p.minutes, p.seconds, p.subseconds);
                     },
                     [](const DurationParamsDays &p) { return fmt::format("P{}D", p.days); },
                     [](const DurationParamsHours &p) { return fmt::format("PT{}H", p.hours); },
                     [](const DurationParamsMinutes &p) { return fmt::format("PT{}M", p.minutes); },
                     [](const DurationParamsSeconds &p) { return fmt::format("PT{}S", p.seconds); }),
                 params);
  //  std::cout << dur_str << std::endl;
  const auto query = fmt::format("CREATE (:{} {{{}: DURATION(\"{}\")}})", group, property, dur_str);
  MaybeExecuteQuery(client, query, "Duration");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
  const auto dur = (*it).second.ValueDuration();
  const auto expected = utils::Duration(utils::Duration(utils::ParseDurationParameters(dur_str)));
  MG_ASSERT(dur.months() == 0, "Received incorrect months in the duration");
  MG_ASSERT(dur.days() == expected.Days(), "Received incorrect days in the duration");
  MG_ASSERT(dur.seconds() == expected.SubDaysAsSeconds(), "Received incorrect seconds in the duration");
  MG_ASSERT(dur.nanoseconds() == expected.SubSecondsAsNanoseconds(), "Received incorrect nanoseconds in the duration");
}

void RoundtripDate(mg::Client &client, const std::string_view group, const std::string_view property,
                   const utils::DateParameters &params) {
  const auto date_str = fmt::format("{:0>2}-{:0>2}-{:0>2}", params.years, params.months, params.days);
  const auto query = fmt::format("CREATE (:{} {{{}: DATE(\"{}\")}})", group, property, date_str);
  MaybeExecuteQuery(client, query, "Date");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
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
                        const LocalTimeParams &params) {
  const auto lt_str =
      fmt::format("{:0>2}:{:0>2}:{:0>2}.{:0>6}", params.hours, params.minutes, params.seconds, params.subseconds);
  const auto query = fmt::format("CREATE (:{} {{{}: LOCALTIME(\"{}\")}})", group, property, lt_str);
  MaybeExecuteQuery(client, query, "LocalTime");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
  const auto lt = (*it).second.ValueLocalTime();
  const auto expected = utils::LocalTime(utils::ParseLocalTimeParameters(lt_str).first);
  MG_ASSERT(lt.nanoseconds() == expected.NanosecondsSinceEpoch(),
            "Received incorrect nanoseconds in the LocalTime roundtrip");
}

void RoundtripLocalDateTime(mg::Client &client, const std::string_view group, const std::string_view property,
                            const utils::DateParameters &d_params, const LocalTimeParams &lt_params) {
  const auto date_str = fmt::format("{:0>2}-{:0>2}-{:0>2}", d_params.years, d_params.months, d_params.days);
  const auto lt_str = fmt::format("{:0>2}:{:0>2}:{:0>2}", lt_params.hours, lt_params.minutes, lt_params.seconds);
  const auto ldt_str = date_str + "T" + lt_str;
  const auto query = fmt::format("CREATE (:{} {{{}: LOCALDATETIME(\"{}\")}})", group, property, ldt_str);
  MaybeExecuteQuery(client, query, "LocalDateTime");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
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
}

void TestLocalDateTime(mg::Client &client) {
  RoundtripLocalDateTime(client, "LDT1", "time", {1200, 8, 9}, {12, 33, 1});
  RoundtripLocalDateTime(client, "LDT2", "time", {1961, 6, 3}, {11, 22, 10});
  RoundtripLocalDateTime(client, "LDT3", "time", {1971, 1, 1}, {15, 16, 2});
  RoundtripLocalDateTime(client, "LDT4", "time", {2000, 1, 1}, {2, 33, 1});
  RoundtripLocalDateTime(client, "LDT5", "time", {2021, 9, 21}, {16, 57, 1});
}

void TestDuration(mg::Client &client) {
  RoundtripDuration(client, "Runner1", "time", DurationParams{3, 5, 6, 2, 1});
  RoundtripDuration(client, "Runner2", "time", DurationParams{8, 9, 8, 4, 12});
  RoundtripDuration(client, "Runner3", "time", DurationParams{10, 10, 12, 44, 222222});
  RoundtripDuration(client, "Runner4", "time", DurationParams{23, 11, 13, 59, 131459});
  RoundtripDuration(client, "Runner5", "time", DurationParams{0, 110, 14, 88, 131459});

  // fractions
  RoundtripDuration(client, "Runner6", "time", DurationParamsDays{2.5});
  RoundtripDuration(client, "Runner7", "time", DurationParamsHours{5.4});
  RoundtripDuration(client, "Runner8", "time", DurationParamsMinutes{6.3});
  RoundtripDuration(client, "Runner9", "time", DurationParamsSeconds{9.5});
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
