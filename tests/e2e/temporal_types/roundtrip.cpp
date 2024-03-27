// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <variant>

#include <gflags/gflags.h>
#include "fmt/format.h"
#include "mgclient.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

DEFINE_uint64(bolt_port, 0, "Bolt port arguments");

void MaybeExecuteQuery(mg::Client &client, const std::string &query, const std::string_view name) {
  auto executed = client.Execute(query);
  MG_ASSERT(executed, "Failed to execute {} query", name);
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
  MG_ASSERT(it != props.end(), "Failed to find property {}", property);
  return it;
}

void RoundtripDuration(mg::Client &client, const std::string_view group, const std::string_view property,
                       const std::string_view dur_str, const memgraph::utils::Duration &expected) {
  const auto query = fmt::format("CREATE (:{} {{{}: DURATION({})}})", group, property, dur_str);
  MaybeExecuteQuery(client, query, "Duration");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
  const auto dur = (*it).second.ValueDuration();
  MG_ASSERT(dur.months() == 0, "Received incorrect months in the duration");
  MG_ASSERT(dur.days() == expected.Days(), "Received incorrect days in the duration");
  MG_ASSERT(dur.seconds() == expected.SubDaysAsSeconds(), "Received incorrect seconds in the duration");
  MG_ASSERT(dur.nanoseconds() == expected.SubSecondsAsNanoseconds(), "Received incorrect nanoseconds in the duration");
}

void RoundtripDate(mg::Client &client, const std::string_view group, const std::string_view property,
                   const std::string_view date_str, const memgraph::utils::Date &expected) {
  const auto query = fmt::format("CREATE (:{} {{{}: DATE({})}})", group, property, date_str);
  MaybeExecuteQuery(client, query, "Date");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
  const auto date = (*it).second.ValueDate();
  MG_ASSERT(date.days() == expected.DaysSinceEpoch(), "Received incorrect days in the date roundtrip");
}

struct LocalTimeParams {
  int64_t hour{0};
  int64_t minute{0};
  int64_t second{0};
  int64_t subsecond{0};
};

void RoundtripLocalTime(mg::Client &client, const std::string_view group, const std::string_view property,
                        const std::string_view lt_str, const memgraph::utils::LocalTime &expected) {
  const auto query = fmt::format("CREATE (:{} {{{}: LOCALTIME({})}})", group, property, lt_str);
  MaybeExecuteQuery(client, query, "LocalTime");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
  const auto lt = (*it).second.ValueLocalTime();
  MG_ASSERT(lt.nanoseconds() == expected.NanosecondsSinceEpoch(),
            "Received incorrect nanoseconds in the LocalTime roundtrip");
}

void RoundtripLocalDateTime(mg::Client &client, const std::string_view group, const std::string_view property,
                            const std::string_view ldt_str, const memgraph::utils::LocalDateTime &expected) {
  const auto query = fmt::format("CREATE (:{} {{{}: LOCALDATETIME({})}})", group, property, ldt_str);
  MaybeExecuteQuery(client, query, "LocalDateTime");
  const auto result = MaybeExecuteMatch(client, group);
  const auto node = (*result)[0][0].ValueNode();
  const auto props = node.properties();
  const auto it = GetItFromNodeProperty(props, property);
  const auto ldt = (*it).second.ValueLocalDateTime();
  MG_ASSERT(ldt.seconds() == expected.SecondsSinceEpoch(), "Received incorrect seconds in the LocalDateTime roundtrip");
  MG_ASSERT(ldt.nanoseconds() == expected.SubSecondsAsNanoseconds(),
            "Received incorrect nanoseconds in the LocalDateTime roundtrip");
}

void TestDate(mg::Client &client) {
  auto date_query = [](auto year, auto month, auto day) {
    return fmt::format("\"{:0>2}-{:0>2}-{:0>2}\"", year, month, day);
  };
  auto date_query_map = [](auto year, auto month, auto day) {
    return fmt::format("{{year:{}, month:{}, day:{}}}", year, month, day);
  };
  RoundtripDate(client, "Person1", "dob", date_query(1960, 1, 12), memgraph::utils::Date({1960, 1, 12}));
  RoundtripDate(client, "Person2", "dob", date_query(1970, 1, 1), memgraph::utils::Date({1970, 1, 1}));
  RoundtripDate(client, "Person3", "dob", date_query(1971, 2, 2), memgraph::utils::Date({1971, 2, 2}));
  RoundtripDate(client, "Person4", "dob", date_query(2021, 12, 9), memgraph::utils::Date({2021, 12, 9}));

  RoundtripDate(client, "PersonMap1", "dob", date_query_map(1970, 1, 1), memgraph::utils::Date({1970, 1, 1}));
  RoundtripDate(client, "PersonMap2", "dob", date_query_map(1971, 6, 5), memgraph::utils::Date({1971, 6, 5}));
  RoundtripDate(client, "PersonMap3", "dob", date_query_map(2000, 8, 9), memgraph::utils::Date({2000, 8, 9}));
  RoundtripDate(client, "PersonMap4", "dob", date_query_map(2021, 12, 9), memgraph::utils::Date({2021, 12, 9}));
}

void TestLocalTime(mg::Client &client) {
  auto lt = [](auto h, auto m, auto s, auto ss) { return fmt::format("{:2>2}:{:0>2}:{:0>2}.{:0>6}", h, m, s, ss); };
  auto lt_query = [](auto lt_as_str) { return fmt::format("\"{}\"", lt_as_str); };
  auto lt_query_map = [](int h = 0, int m = 0, int s = 0, int ml = 0, int mi = 0) {
    return fmt::format("{{hour:{}, minute:{}, second:{}, millisecond:{}, microsecond:{}}}", h, m, s, ml, mi);
  };

  const auto parse = [](const std::string_view query) {
    return memgraph::utils::LocalTime(memgraph::utils::ParseLocalTimeParameters(fmt::format("{}", query)).first);
  };

  const auto str1 = lt(1, 3, 3, 33);
  RoundtripLocalTime(client, "LT1", "time", lt_query(str1), parse(str1));
  const auto str2 = lt(13, 4, 44, 1002);
  RoundtripLocalTime(client, "LT2", "time", lt_query(str2), parse(str2));
  const auto str3 = lt(18, 22, 21, 68010);
  RoundtripLocalTime(client, "LT3", "time", lt_query(str3), parse(str3));
  const auto str4 = lt(1, 3, 3, 33);
  RoundtripLocalTime(client, "LT4", "time", lt_query(str4), parse(str4));

  const auto str5 = lt_query_map(10, 4, 22, 33, 99);
  RoundtripLocalTime(client, "LT5", "time", str5, memgraph::utils::LocalTime({10, 4, 22, 33, 99}));
  const auto str6 = lt_query_map(0, 0, 21, 12, 88);
  RoundtripLocalTime(client, "LT6", "time", str6, memgraph::utils::LocalTime({0, 0, 21, 12, 88}));
  const auto str7 = lt_query_map(8, 4, 22, 33, 99);
  RoundtripLocalTime(client, "LT7", "time", str7, memgraph::utils::LocalTime({8, 4, 22, 33, 99}));
  const auto str8 = lt_query_map(23, 1, 0, 0, 0);
  RoundtripLocalTime(client, "LT8", "time", str8, memgraph::utils::LocalTime({23, 1, 0, 0, 0}));
}

void TestLocalDateTime(mg::Client &client) {
  auto ldt = [](auto y, auto mo, auto d, auto h, auto m, auto s) {
    return fmt::format("{:0>2}-{:0>2}-{:0>2}T{:0>2}:{:0>2}:{:0>2}", y, mo, d, h, m, s);
  };
  auto parse = [](const std::string_view str) {
    const auto [dt, lt] = memgraph::utils::ParseLocalDateTimeParameters(str);
    return memgraph::utils::LocalDateTime(dt, lt);
  };
  auto ldt_query = [](const std::string_view str) { return fmt::format("\"{}\"", str); };
  const auto str = ldt(1200, 8, 9, 12, 33, 1);
  RoundtripLocalDateTime(client, "LDT1", "time", ldt_query(str), parse(str));
  const auto str1 = ldt(1961, 6, 3, 11, 22, 10);
  RoundtripLocalDateTime(client, "LDT2", "time", ldt_query(str1), parse(str1));
  const auto str2 = ldt(1971, 1, 1, 15, 16, 2);
  RoundtripLocalDateTime(client, "LDT3", "time", ldt_query(str2), parse(str2));
  const auto str3 = ldt(2000, 1, 1, 2, 33, 1);
  RoundtripLocalDateTime(client, "LDT4", "time", ldt_query(str3), parse(str3));
  const auto str4 = ldt(2021, 9, 21, 16, 57, 1);
  RoundtripLocalDateTime(client, "LDT5", "time", ldt_query(str4), parse(str4));

  RoundtripLocalDateTime(client, "Map_LDT1", "time", "{year:1960, month:10, day:1}",
                         memgraph::utils::LocalDateTime({1960, 10, 1}, {}));
  RoundtripLocalDateTime(client, "Map_LDT2", "time", "{year:1960, month:10, day:1, hour:1, minute:2, second:33}",
                         memgraph::utils::LocalDateTime({1960, 10, 1}, {1, 2, 33}));
  RoundtripLocalDateTime(client, "Map_LDT3", "time", "{hour:10}", memgraph::utils::LocalDateTime({}, {.hour = 10}));
  RoundtripLocalDateTime(client, "Map_LDT4", "time", "{day:2}", memgraph::utils::LocalDateTime({.day = 2}, {}));
}

void TestDuration(mg::Client &client) {
  const auto dur = [](auto d, auto h, auto m, auto s, auto ss) {
    return fmt::format("\"P{}DT{}H{}M{}.{}S\"", d, h, m, s, ss);
  };

  RoundtripDuration(client, "Runner1", "time", dur(3, 5, 6, 2, 1), memgraph::utils::Duration({3, 5, 6, 2.1}));
  RoundtripDuration(client, "Runner2", "time", dur(8, 9, 8, 4, 12), memgraph::utils::Duration({8, 9, 8, 4.12}));
  RoundtripDuration(client, "Runner3", "time", dur(10, 10, 12, 44, 44), memgraph::utils::Duration({10, 10, 12, 44.44}));
  RoundtripDuration(client, "Runner4", "time", dur(23, 11, 13, 59, 100000),
                    memgraph::utils::Duration({23, 11, 13, 59, 100}));
  RoundtripDuration(client, "Runner5", "time", dur(0, 110, 14, 88, 400000),
                    memgraph::utils::Duration({0, 110, 14, 88, 400}));

  // fractions
  RoundtripDuration(client, "Runner6", "time", "\"P4.5D\"",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.day = 4.5}));
  RoundtripDuration(client, "Runner7", "time", "\"PT9.3H\"",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.hour = 9.3}));
  RoundtripDuration(client, "Runner8", "time", "\"PT4.2M\"",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.minute = 4.2}));
  RoundtripDuration(client, "Runner9", "time", "\"PT8.4S\"",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.second = 8.4}));

  RoundtripDuration(client, "RunnerMap1", "time",
                    "{day:0, hour:4, minute:1, second:44, millisecond:44, microsecond:22}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{0, 4, 1, 44, 44, 22}));
  RoundtripDuration(client, "RunnerMap2", "time", "{day:15}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{15}));
  RoundtripDuration(client, "RunnerMap3", "time", "{hour:2.5}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.hour = 2.5}));
  RoundtripDuration(client, "RunnerMap4", "time", "{minute:10.5, second:44}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.minute = 10.5, .second = 44}));

  RoundtripDuration(client, "NegRunnerMap1", "time",
                    "{day:0, hour:-1, minute:-2, second:-20, millisecond:-4, microsecond:-15}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{0, -1, -2, -20, -4, -15}));
  RoundtripDuration(client, "NegRunnerMap2", "time", "{day:-15}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{-15}));
  RoundtripDuration(client, "NegRunnerMap3", "time", "{hour:-2.5}",
                    memgraph::utils::Duration(memgraph::utils::DurationParameters{.hour = -2.5}));
}

int main(int argc, char **argv) {
  gflags::SetUsageMessage("Memgraph E2E temporal types roundtrip");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  MG_ASSERT(FLAGS_bolt_port != 0);
  memgraph::logging::RedirectToStderr();

  mg::Client::Init();
  auto client = mg::Client::Connect({.port = static_cast<uint16_t>(FLAGS_bolt_port)});
  MG_ASSERT(client, "Failed to connect with memgraph");
  TestDate(*client);
  TestLocalTime(*client);
  TestLocalDateTime(*client);
  TestDuration(*client);
  return 0;
}
