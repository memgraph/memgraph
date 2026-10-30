// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "flags/logging.hpp"
#include "utils/exit_codes.hpp"
#include "utils/session_context.hpp"
#include "utils/structured_logging.hpp"

#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/ostream_sink.h>
#include <unistd.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

DECLARE_string(log_file);
DECLARE_string(log_format);
DECLARE_string(logger_type);
DECLARE_uint64(log_retention_days);

// Counter-incrementing fmt formatter — proves the lazy-format contract.
struct CountingFormattable {
  static inline int format_count = 0;
};

template <>
struct fmt::formatter<CountingFormattable> : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const CountingFormattable & /*unused*/, FormatContext &ctx) const {
    ++CountingFormattable::format_count;
    return fmt::formatter<std::string_view>::format("counted", ctx);
  }
};

namespace {

namespace fs = std::filesystem;

class CleanLogsDirTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = fs::temp_directory_path() / ("mg_log_test_" + std::to_string(::getpid()));
    fs::create_directories(test_dir_);
    original_log_file_ = FLAGS_log_file;
    original_retention_days_ = FLAGS_log_retention_days;
  }

  void TearDown() override {
    FLAGS_log_file = original_log_file_;
    FLAGS_log_retention_days = original_retention_days_;
    std::error_code ec;
    fs::remove_all(test_dir_, ec);
  }

  void CreateFile(const std::string &name, std::chrono::days age) {
    auto path = test_dir_ / name;
    std::ofstream{path} << "log content";
    auto const old_time = fs::file_time_type::clock::now() - age;
    fs::last_write_time(path, old_time);
  }

  std::vector<std::string> RemainingFiles() {
    std::vector<std::string> result;
    for (auto const &entry : fs::directory_iterator(test_dir_)) {
      if (entry.is_regular_file()) {
        result.emplace_back(entry.path().filename().string());
      }
    }
    std::sort(result.begin(), result.end());
    return result;
  }

  fs::path test_dir_;
  std::string original_log_file_;
  uint64_t original_retention_days_{};
};

TEST_F(CleanLogsDirTest, EmptyLogFileFlag) {
  FLAGS_log_file = "";
  CreateFile("old.log", std::chrono::days{10});
  memgraph::flags::CleanLogsDir();
  EXPECT_EQ(RemainingFiles().size(), 1);
}

TEST_F(CleanLogsDirTest, AllFilesWithinRetention) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 5;

  CreateFile("recent_1.log", std::chrono::days{2});
  CreateFile("recent_2.log", std::chrono::days{1});
  CreateFile("recent_3.log", std::chrono::days{0});

  memgraph::flags::CleanLogsDir();

  EXPECT_EQ(RemainingFiles().size(), 3);
}

TEST_F(CleanLogsDirTest, OldFilesDeleted) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 2;

  CreateFile("old_1.log", std::chrono::days{7});
  CreateFile("old_2.log", std::chrono::days{6});
  CreateFile("old_3.log", std::chrono::days{5});
  CreateFile("old_4.log", std::chrono::days{3});
  CreateFile("recent_1.log", std::chrono::days{1});
  CreateFile("recent_2.log", std::chrono::days{0});

  memgraph::flags::CleanLogsDir();

  auto remaining = RemainingFiles();
  EXPECT_EQ(remaining.size(), 2);
  EXPECT_EQ(remaining[0], "recent_1.log");
  EXPECT_EQ(remaining[1], "recent_2.log");
}

TEST_F(CleanLogsDirTest, AllFilesOld) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 1;

  CreateFile("old_1.log", std::chrono::days{10});
  CreateFile("old_2.log", std::chrono::days{9});
  CreateFile("old_3.log", std::chrono::days{8});

  memgraph::flags::CleanLogsDir();

  EXPECT_TRUE(RemainingFiles().empty());
}

TEST_F(CleanLogsDirTest, DirectoriesAreNotDeleted) {
  FLAGS_log_file = (test_dir_ / "memgraph.log").string();
  FLAGS_log_retention_days = 1;

  CreateFile("old.log", std::chrono::days{10});
  fs::create_directory(test_dir_ / "subdir");

  memgraph::flags::CleanLogsDir();

  auto remaining = RemainingFiles();
  EXPECT_TRUE(remaining.empty());
  // subdir should still exist
  EXPECT_TRUE(fs::exists(test_dir_ / "subdir"));
}

TEST_F(CleanLogsDirTest, NonExistentDirectory) {
  FLAGS_log_file = "/tmp/mg_nonexistent_dir_test/memgraph.log";
  FLAGS_log_retention_days = 2;
  EXPECT_NO_THROW(memgraph::flags::CleanLogsDir());
}

TEST(InitializeLoggerDeathTest, UnwritableLogFileExitsCleanlyWithActionableMessage) {
  if (geteuid() == 0) GTEST_SKIP() << "permission checks do not apply to root";

  auto const dir = fs::temp_directory_path() / "mg_unit_logging_unwritable";
  fs::create_directories(dir);
  fs::permissions(dir, fs::perms::owner_read | fs::perms::owner_exec);

  auto const original_log_file = FLAGS_log_file;
  FLAGS_log_file = (dir / "memgraph.log").string();
  EXPECT_EXIT(memgraph::flags::InitializeLogger(),
              testing::ExitedWithCode(memgraph::utils::AsExitStatus(memgraph::utils::ExitCode::LogFileNotWritable)),
              "Failed to open log file");
  FLAGS_log_file = original_log_file;

  fs::permissions(dir, fs::perms::owner_all);
  fs::remove_all(dir);
}

TEST(LogFormatFlag, DefaultsToTextAndRejectsUnsupportedFormats) {
  gflags::FlagSaver saved_flags;
  gflags::CommandLineFlagInfo info;
  ASSERT_TRUE(gflags::GetCommandLineFlagInfo("log_format", &info));
  EXPECT_EQ(info.default_value, "text");
  EXPECT_FALSE(gflags::SetCommandLineOption("log_format", "json").empty());
  EXPECT_EQ(FLAGS_log_format, "json");
  EXPECT_FALSE(gflags::SetCommandLineOption("log_format", "text").empty());

  testing::internal::CaptureStderr();
  EXPECT_TRUE(gflags::SetCommandLineOption("log_format", "xml").empty());
  EXPECT_TRUE(gflags::SetCommandLineOption("log_format", "").empty());
  const auto error = testing::internal::GetCapturedStderr();
  EXPECT_NE(error.find("to be 'text' or 'json'"), std::string::npos);
  EXPECT_EQ(FLAGS_log_format, "text");
}

class LoggerFormatTest : public ::testing::TestWithParam<const char *> {
 protected:
  void SetUp() override {
    previous_logger_ = spdlog::default_logger();
    previous_stderr_sink_ = memgraph::flags::stderr_sink();
    memgraph::flags::stderr_sink() = std::make_shared<spdlog::sinks::stderr_color_sink_st>();
    test_dir_ = fs::temp_directory_path() / ("mg_logger_format_test_" + std::to_string(::getpid()));
    fs::create_directories(test_dir_);
    FLAGS_log_file = (test_dir_ / "memgraph.log").string();
    FLAGS_log_format = "text";
    FLAGS_logger_type = GetParam();
  }

  void TearDown() override {
    StopLogger();
    memgraph::flags::stderr_sink() = previous_stderr_sink_;
    fs::remove_all(test_dir_);
  }

  void StartLogger() {
    memgraph::flags::InitializeLogger();
    memgraph::flags::TurnOnStdErr();
  }

  void StopLogger() {
    // shutdown drains the async queue before files and captured stderr are read.
    spdlog::shutdown();
    spdlog::set_default_logger(previous_logger_);
  }

  std::string ReadLog() const {
    std::string contents;
    for (const auto &entry : fs::directory_iterator(test_dir_)) {
      std::ifstream input(entry.path());
      contents.append(std::istreambuf_iterator<char>{input}, std::istreambuf_iterator<char>{});
    }
    return contents;
  }

  gflags::FlagSaver saved_flags_;
  std::shared_ptr<spdlog::logger> previous_logger_;
  spdlog::sink_ptr previous_stderr_sink_;
  fs::path test_dir_;
};

TEST_P(LoggerFormatTest, DefaultTextOutputIsPreserved) {
  StartLogger();
  testing::internal::CaptureStderr();
  spdlog::warn("text compatibility marker");
  StopLogger();
  const auto stderr_output = testing::internal::GetCapturedStderr();
  const auto file_output = ReadLog();
  EXPECT_EQ(stderr_output, file_output);
  EXPECT_NE(file_output.find("[memgraph_log] [warning] text compatibility marker\n"), std::string::npos);
}

TEST_P(LoggerFormatTest, JsonIsAppliedToFileAndStderr) {
  FLAGS_log_format = "json";
  StartLogger();
  testing::internal::CaptureStderr();
  const std::string message = "quoted \"message\"\nsecond line\\path";
  spdlog::warn("{}", message);
  StopLogger();
  const auto stderr_output = testing::internal::GetCapturedStderr();
  const auto file_output = ReadLog();
  EXPECT_EQ(stderr_output, file_output);
  EXPECT_EQ(std::count(file_output.begin(), file_output.end(), '\n'), 1);
  const auto record = nlohmann::json::parse(file_output);
  EXPECT_EQ(record.at("message"), message);
  EXPECT_EQ(record.at("level"), "warning");
  EXPECT_EQ(record.at("logger"), "memgraph_log");
}

TEST_P(LoggerFormatTest, AddedSinksKeepTheirFormatterAndOriginalPayload) {
  FLAGS_log_format = "json";
  StartLogger();
  memgraph::flags::TurnOffStdErr();
  std::ostringstream stream;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream);
  sink->set_pattern("%v");
  memgraph::flags::AddLoggerSink(sink);
  spdlog::warn("unchanged payload");
  StopLogger();
  EXPECT_EQ(stream.str(), "unchanged payload\n");
  EXPECT_EQ(nlohmann::json::parse(ReadLog()).at("message"), "unchanged payload");
}

TEST_P(LoggerFormatTest, StructuredEventsReachAddedSinksWithoutTheirTransportEnvelope) {
  FLAGS_log_format = "json";
  StartLogger();
  memgraph::flags::TurnOffStdErr();
  std::ostringstream stream;
  auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(stream);
  sink->set_pattern("%v");
  memgraph::flags::AddLoggerSink(sink);
  memgraph::logging::LogEvent(
      spdlog::level::warn, "query.failed", "original diagnostic", {{"session.id", "session-1"}});
  StopLogger();
  EXPECT_EQ(stream.str(), "original diagnostic\n");
  const auto record = nlohmann::json::parse(ReadLog());
  EXPECT_EQ(record.at("event").at("name"), "query.failed");
  EXPECT_EQ(record.at("attributes").at("session.id"), "session-1");
}

INSTANTIATE_TEST_SUITE_P(LoggerTypes, LoggerFormatTest, ::testing::Values("sync", "async"));

using memgraph::logging::EmitSessionTraceEvent;
using memgraph::logging::ScopedSessionLog;
using memgraph::logging::SessionLogContext;

std::string TraceTags(const SessionLogContext &ctx) {
  fmt::memory_buffer buf;
  ctx.AppendTraceTags(buf);
  return std::string(buf.data(), buf.size());
}

TEST(SessionLogContext, TraceTagComposition) {
  SessionLogContext ctx;
  ctx.SetSessionUuid("abc");
  ctx.SetTxId(42);
  EXPECT_EQ(TraceTags(ctx), "[session=abc] [tx=42]");

  ctx.SetUser("alice");
  EXPECT_EQ(TraceTags(ctx), "[session=abc] [user=alice] [tx=42]");

  // Clearing a middle field must not leave a dangling separator or empty token.
  ctx.ClearUser();
  EXPECT_EQ(TraceTags(ctx), "[session=abc] [tx=42]");
}

TEST(ScopedSessionLog, NestingAndUnwindRestore) {
  SessionLogContext outer;
  SessionLogContext inner;
  EXPECT_EQ(ScopedSessionLog::Current(), nullptr);
  {
    ScopedSessionLog g_outer(&outer);
    EXPECT_EQ(ScopedSessionLog::Current(), &outer);
    try {
      ScopedSessionLog g_inner(&inner);
      ASSERT_EQ(ScopedSessionLog::Current(), &inner);
      throw std::runtime_error("boom");
    } catch (const std::runtime_error &) {
      // g_inner's dtor must restore on unwind, else a dangling TLS ptr leaks across messages.
    }
    EXPECT_EQ(ScopedSessionLog::Current(), &outer);
  }
  EXPECT_EQ(ScopedSessionLog::Current(), nullptr);
}

// Captures the raw (pre-pattern) payload of every emitted log message.
class CapturingSink : public spdlog::sinks::base_sink<std::mutex> {
 public:
  std::vector<std::string> messages;

 protected:
  void sink_it_(const spdlog::details::log_msg &msg) override {
    messages.emplace_back(msg.payload.data(), msg.payload.size());
  }

  void flush_() override {}
};

class SessionTraceEmitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    previous_logger_ = spdlog::default_logger();
    sink_ = std::make_shared<CapturingSink>();
    auto logger = std::make_shared<spdlog::logger>("session_trace_test", sink_);
    logger->set_level(spdlog::level::trace);
    spdlog::set_default_logger(logger);
    CountingFormattable::format_count = 0;
  }

  void TearDown() override { spdlog::set_default_logger(previous_logger_); }

  std::shared_ptr<CapturingSink> sink_;
  std::shared_ptr<spdlog::logger> previous_logger_;
};

TEST_F(SessionTraceEmitTest, NoActiveContextEmitsNothing) {
  ASSERT_EQ(ScopedSessionLog::Current(), nullptr);
  EmitSessionTraceEvent("hello {}", 1);
  EXPECT_TRUE(sink_->messages.empty());
}

TEST_F(SessionTraceEmitTest, TraceEnabledEmitsTaggedMessage) {
  SessionLogContext ctx;
  ctx.SetSessionUuid("s1");
  ctx.SetTxId(7);
  ctx.SetTrace(true);
  ScopedSessionLog guard(&ctx);

  EmitSessionTraceEvent("hello {}", 42);

  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0], "[session=s1] [tx=7] hello 42");
}

// Lazy-format regression guard: closed gate ⇒ args not formatted.
TEST_F(SessionTraceEmitTest, DisabledDoesNotFormatArguments) {
  SessionLogContext ctx;
  ctx.SetSessionUuid("s1");
  ScopedSessionLog guard(&ctx);  // disabled

  EmitSessionTraceEvent("value {}", CountingFormattable{});

  EXPECT_EQ(CountingFormattable::format_count, 0);
  EXPECT_TRUE(sink_->messages.empty());
}

TEST_F(SessionTraceEmitTest, EnabledFormatsArgumentsExactlyOnce) {
  SessionLogContext ctx;
  ctx.SetTrace(true);
  ScopedSessionLog guard(&ctx);

  EmitSessionTraceEvent("value {}", CountingFormattable{});

  EXPECT_EQ(CountingFormattable::format_count, 1);
}

// --log-level above INFO filters trace events and skips arg formatting entirely.
TEST_F(SessionTraceEmitTest, LevelAboveInfoSkipsArgumentFormatting) {
  spdlog::default_logger()->set_level(spdlog::level::warn);
  SessionLogContext ctx;
  ctx.SetTrace(true);
  ScopedSessionLog guard(&ctx);

  EmitSessionTraceEvent("value {}", CountingFormattable{});

  EXPECT_EQ(CountingFormattable::format_count, 0);
  EXPECT_TRUE(sink_->messages.empty());
}

// ResetForConnectionReuse clears the session overlay (SET SESSION TRACE/SETTING) but keeps
// connection identity and auth user.
TEST(SessionLogContext, ResetForConnectionReuseClearsOverlayButKeepsIdentity) {
  SessionLogContext ctx;
  ctx.SetSessionUuid("conn-1");
  ctx.SetUser("alice");
  ctx.SetTrace(true);
  ctx.SetSetting("query-log-min-duration-ms", "500");
  ASSERT_TRUE(ctx.trace_enabled());
  ASSERT_TRUE(ctx.GetSetting("query-log-min-duration-ms").has_value());

  ctx.ResetForConnectionReuse();

  EXPECT_FALSE(ctx.trace_enabled());
  EXPECT_FALSE(ctx.GetSetting("query-log-min-duration-ms").has_value());
  EXPECT_EQ(ctx.session_uuid(), "conn-1");  // physical-connection identity preserved
  EXPECT_EQ(ctx.user(), "alice");           // auth user preserved (reset via LogOff, not RESET)
}

// Pre-formatted overload: braces in the payload are emitted verbatim, not as format spec.
TEST_F(SessionTraceEmitTest, PreFormattedOverloadEmitsBracesVerbatim) {
  SessionLogContext ctx;
  ctx.SetSessionUuid("s1");
  ctx.SetTxId(9);
  ctx.SetTrace(true);
  ScopedSessionLog guard(&ctx);

  EmitSessionTraceEvent(std::string_view{"oops { bad fmt }"});

  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0], "[session=s1] [tx=9] oops { bad fmt }");
}

// Call-site gate must mirror the inner gate (incl. log level), else callers build trace args for nothing.
TEST_F(SessionTraceEmitTest, IsSessionTraceEnabledRespectsLogLevel) {
  SessionLogContext ctx;
  ctx.SetTrace(true);
  ScopedSessionLog guard(&ctx);

  spdlog::default_logger()->set_level(spdlog::level::trace);
  EXPECT_TRUE(memgraph::logging::IsSessionTraceEnabled());

  spdlog::default_logger()->set_level(spdlog::level::warn);
  EXPECT_FALSE(memgraph::logging::IsSessionTraceEnabled());
}

// --- Per-session settings overlay -------------------------------------------

TEST(SessionLogContextOverlay, AbsentKeyReturnsNullopt) {
  SessionLogContext ctx;
  EXPECT_FALSE(ctx.GetSetting("log.min_duration_ms").has_value());
}

TEST(SessionLogContextOverlay, SetAndGet) {
  SessionLogContext ctx;
  ctx.SetSetting("log.min_duration_ms", "250");
  auto v = ctx.GetSetting("log.min_duration_ms");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, "250");
}

TEST(SessionLogContextOverlay, ResetClearsValue) {
  SessionLogContext ctx;
  ctx.SetSetting("log.failed_queries", "true");
  ctx.ResetSetting("log.failed_queries");
  EXPECT_FALSE(ctx.GetSetting("log.failed_queries").has_value());
}

TEST(SessionLogContextOverlay, SetOverwritesExistingValue) {
  SessionLogContext ctx;
  ctx.SetSetting("log.min_duration_ms", "100");
  ctx.SetSetting("log.min_duration_ms", "500");
  auto v = ctx.GetSetting("log.min_duration_ms");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(*v, "500");
}

// --- Slow/failed-query emit helpers -----------------------------------------

class QueryLogEmitTest : public SessionTraceEmitTest {};

TEST_F(QueryLogEmitTest, SlowQueryHeaderOnlyWhenNoPlan) {
  memgraph::logging::EmitSlowQueryLog("alice", "memgraph", "MATCH (n) RETURN n", 1234, std::nullopt);
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0], "[slow-query] duration_ms=1234 user=alice db=memgraph query=\"MATCH (n) RETURN n\"");
}

TEST_F(QueryLogEmitTest, SlowQueryIncludesPlanBlockWhenProvided) {
  const std::string plan = "ScanAll\n  Once\n";
  memgraph::logging::EmitSlowQueryLog("alice", "memgraph", "MATCH (n) RETURN n", 50, std::string_view{plan});
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_NE(sink_->messages[0].find("[slow-query] duration_ms=50"), std::string::npos);
  // Each non-empty source line is prefixed with two extra spaces; pretty-print's
  // own indentation (the "  " in "  Once") is preserved on top.
  EXPECT_NE(sink_->messages[0].find("\nPLAN:\n  ScanAll\n    Once"), std::string::npos);
}

TEST_F(QueryLogEmitTest, SlowQueryEscapesEmbeddedQuotes) {
  memgraph::logging::EmitSlowQueryLog("bob", "memgraph", R"(RETURN "hi")", 1, std::nullopt);
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_NE(sink_->messages[0].find(R"(query="RETURN \"hi\"")"), std::string::npos);
}

TEST_F(QueryLogEmitTest, SlowQueryEscapesEmbeddedBackslash) {
  // Both the backslash and the quotes must be escaped, so a literal `\"` becomes `\\\"`.
  memgraph::logging::EmitSlowQueryLog("bob", "memgraph", R"(RETURN "a\b")", 1, std::nullopt);
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0], R"([slow-query] duration_ms=1 user=bob db=memgraph query="RETURN \"a\\b\"")");
}

TEST_F(QueryLogEmitTest, FailedQueryEscapesBackslashInErrorAndQuery) {
  memgraph::logging::EmitFailedQueryLog("alice", "memgraph", R"(LOAD CSV FROM "c:\tmp")", R"(bad path c:\tmp)");
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0],
            R"([failed-query] user=alice db=memgraph error="bad path c:\\tmp" query="LOAD CSV FROM \"c:\\tmp\"")");
}

TEST_F(QueryLogEmitTest, FieldsEscapeNewlinesAndTabsOntoSingleLine) {
  // A multi-line query and a tab-laden error must stay on one physical line so
  // the quoted fields remain self-delimiting; only the PLAN block is multi-line.
  memgraph::logging::EmitFailedQueryLog("alice", "memgraph", "MATCH (n)\nRETURN n", "boom\ttab");
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0].find('\n'), std::string::npos);
  EXPECT_EQ(sink_->messages[0],
            R"([failed-query] user=alice db=memgraph error="boom\ttab" query="MATCH (n)\nRETURN n")");
}

TEST_F(QueryLogEmitTest, FailedQueryLineHasTagAndFields) {
  memgraph::logging::EmitFailedQueryLog("alice", "memgraph", "RETURN 1/0", "Division by zero");
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_EQ(sink_->messages[0], R"([failed-query] user=alice db=memgraph error="Division by zero" query="RETURN 1/0")");
}

TEST_F(QueryLogEmitTest, FailedQueryHandlesNoneDb) {
  memgraph::logging::EmitFailedQueryLog("", "<none>", "FOO", "Syntax error");
  ASSERT_EQ(sink_->messages.size(), 1u);
  EXPECT_NE(sink_->messages[0].find("user= db=<none>"), std::string::npos);
}

}  // namespace
