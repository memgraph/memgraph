// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/time_to_live/time_to_live.hpp"

#include <chrono>
#include <memory>
#include <optional>
#include <thread>

#include "query/discard_value_stream.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/typed_value.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

#ifdef MG_ENTERPRISE

namespace memgraph::metrics {
extern const Event DeletedNodes;
extern const Event DeletedEdges;
}  // namespace memgraph::metrics

namespace {
template <typename T>
int GetPart(auto &current) {
  const int whole_part = std::chrono::duration_cast<T>(current).count();
  current -= T{whole_part};
  return whole_part;
}
}  // namespace

namespace memgraph::query::ttl {

template <typename TDbAccess>
void TTL::Setup_(TDbAccess db_acc, InterpreterContext *interpreter_context, const bool should_run_edge_ttl) {
  if (!enabled_) {
    throw TtlException("TTL not enabled!");
  }
  if (ttl_.IsRunning()) {
    throw TtlException("TTL already running!");
  }
  if (!info_) {
    throw TtlException("TTL not configured!");
  }

  auto interpreter =
      std::shared_ptr<query::Interpreter>(new Interpreter(interpreter_context, db_acc), [interpreter_context](auto *p) {
        p->Abort();
        interpreter_context->interpreters->erase(p);
        delete p;
      });

  // NOTE: We generate an empty user to avoid generating interpreter's fine grained access control.
  // The TTL query already protects who is configuring it, so no need to auth here
  // TODO: Fix auth inconsistency
  interpreter->SetUser(interpreter_context->auth_checker->GenEmptyUser());
  interpreter->OnChangeCB([](auto) { return false; });  // Disable database change
                                                        // register new interpreter into interpreter_context
  interpreter_context->interpreters->insert(interpreter.get());

  auto TTL = [interpreter = std::move(interpreter), should_run_edge_ttl]() {
    memgraph::query::DiscardValueResultStream result_stream;
    bool finished_vertex = false;
    bool finished_edge = !should_run_edge_ttl;
    // Using microseconds to be aligned with timestamp() query, could just use seconds
    const auto now = std::chrono::system_clock::now();
    const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());

    auto get_value = [](auto map, std::string_view key) {
      int64_t n = 0;
      // Empty set will not have a stats field (nothing happened, so nothing to report)
      const auto stats = map.find("stats");
      if (stats != map.end()) {
        // TODO: C++26 will handle transparent comparator with at()
        n = stats->second.ValueMap().find(key)->second.ValueInt();
      }
      return n;
    };

    spdlog::trace("Running TTL at {}", now);
    while (!finished_vertex || !finished_edge) {
      try {
        int n_deleted = 0;
        int n_edges_deleted = 0;
        interpreter->BeginTransaction();
        // First run vertex TTL as that might already delete edges scheduled to be deleted by the edge TTL
        if (!finished_vertex) {
          auto prepare_result =
              interpreter->Prepare("MATCH (n:TTL) WHERE n.ttl < $now WITH n LIMIT $batch DETACH DELETE n;",
                                   [now_us](auto) {
                                     UserParameters params;
                                     params.emplace("now", now_us.count());
                                     params.emplace("batch", 10000);
                                     return params;
                                   },
                                   {});
          const auto pull_res = interpreter->PullAll(&result_stream);
          n_deleted = get_value(pull_res, "nodes-deleted");
          n_edges_deleted = get_value(pull_res, "relationships-deleted");
          finished_vertex = !pull_res.at("has_more").ValueBool() && n_deleted == 0;
        } else if (!finished_edge) {
          auto prepare_result =
              interpreter->Prepare("MATCH ()-[e]->() WHERE e.ttl < $now WITH e LIMIT $batch DETACH DELETE e;",
                                   [now_us](auto) {
                                     UserParameters params;
                                     params.emplace("now", now_us.count());
                                     params.emplace("batch", 10000);
                                     return params;
                                   },
                                   {});
          const auto pull_res = interpreter->PullAll(&result_stream);
          n_edges_deleted = get_value(pull_res, "relationships-deleted");
          finished_edge = !pull_res.at("has_more").ValueBool() && n_edges_deleted == 0;
        } else {
          DMG_ASSERT(false, "Unsupported TTL state.");
        }
        spdlog::trace("Committing TTL batch transaction");
        interpreter->CommitTransaction();
        spdlog::trace("Committed TTL batch deleted {} vertices and {} edges", n_deleted, n_edges_deleted);
        // Telemetry
        memgraph::metrics::IncrementCounter(memgraph::metrics::DeletedNodes, n_deleted);
        memgraph::metrics::IncrementCounter(memgraph::metrics::DeletedEdges, n_edges_deleted);
      } catch (const TransactionSerializationException &e) {
        spdlog::trace("TTL serialization error; Aborting and retrying...");
        interpreter->Abort();  // Retry later
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
      } catch (const WriteQueryOnMainException & /* not used */) {
        // MAIN not ready to handle write queries; abort and try later
        spdlog::trace("MAIN not ready for write queries. TTL will try again later.");
        interpreter->Abort();  // Retry later
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
        break;
      } catch (const WriteQueryOnReplicaException & /* not used */) {
        // TTL cannot run on a REPLICA; ReplicationHandler needs to pause and restart ttl
        spdlog::trace("TTL on REPLICA is not supported.");
        interpreter->Abort();
        // Shouldn't need this sleep; just make sure replication handler has time to pause
        std::this_thread::sleep_for(std::chrono::seconds{1});
        break;
      } catch (const DatabaseContextRequiredException &e) {
        // No database; we are shutting down
        interpreter->Abort();
        spdlog::trace("No database associated with TTL; shuting down...");
        break;
      }
      std::this_thread::yield();
    }
    spdlog::trace("Finished TTL run from {}", now);
  };

  DMG_ASSERT(info_.period, "Period has to be defined for TTL");
  ttl_.SetInterval(*info_.period, info_.start_time);
  ttl_.Run(db_acc->name() + "-ttl", std::move(TTL));
  Persist_();
}

template <typename TDbAccess>
bool TTL::Restore(TDbAccess db, InterpreterContext *interpreter_context) {
  auto fail = [&](std::string_view field) {
    spdlog::warn("Failed to restore TTL, due to '{}'.", field);
    ttl_.Stop();
    info_ = {};
    enabled_ = false;
    return false;
  };

  try {
    {
      const auto ver = storage_.Get("version");
      if (!ver || *ver != "1.0") {
        return fail("version");
      }
    }
    {
      const auto ena = storage_.Get("enabled");
      if (!ena || (*ena != "false" && *ena != "true")) {
        return fail("enabled");
      }
      enabled_ = *ena == "true";
    }
    {
      const auto per = storage_.Get("period");
      if (!per) {
        return fail("period");
      }
      if (per->empty())
        info_.period = std::nullopt;
      else
        info_.period = TtlInfo::ParsePeriod(*per);
    }
    {
      const auto st = storage_.Get("start_time");
      if (!st) {
        return fail("start_time");
      }
      if (st->empty())
        info_.start_time = std::nullopt;
      else
        info_.start_time = TtlInfo::ParseStartTime(*st);
    }
    {
      const auto run = storage_.Get("running");
      if (!run || (*run != "false" && *run != "true")) {
        return fail("running");
      }
      if (*run == "true") {
        const bool run_edge_ttl = db->config().salient.items.properties_on_edges &&
                                  db->GetStorageMode() != storage::StorageMode::ON_DISK_TRANSACTIONAL;
        Setup_(db, interpreter_context, run_edge_ttl);
      }
    }
  } catch (TtlException &e) {
    return fail(e.what());
  }
  return true;
}

std::chrono::microseconds TtlInfo::ParsePeriod(std::string_view sv) {
  if (sv.empty()) return {};
  utils::DurationParameters param;
  int val = 0;
  for (const auto c : sv) {
    if (isdigit(c)) {
      val = val * 10 + (int)(c - '0');
    } else {
      switch (tolower(c)) {
        case 'd':
          param.day = val;
          break;
        case 'h':
          param.hour = val;
          break;
        case 'm':
          param.minute = val;
          break;
        case 's':
          param.second = val;
          break;
        default:
          throw TtlException("Badly defined period. Use integers and 'd', 'h', 'm' and 's' to define it.");
      }
      val = 0;
    }
  }
  return std::chrono::microseconds{utils::Duration(param).microseconds};
}

// We do not support microseconds, but are aligning to the timestamp() values
std::string TtlInfo::StringifyPeriod(std::chrono::microseconds us) {
  std::string res;
  if (const auto di = GetPart<std::chrono::days>(us)) {
    res += fmt::format("{}d", di);
  }
  if (const auto hi = GetPart<std::chrono::hours>(us)) {
    res += fmt::format("{}h", hi);
  }
  if (const auto mi = GetPart<std::chrono::minutes>(us)) {
    res += fmt::format("{}m", mi);
  }
  if (const auto si = GetPart<std::chrono::seconds>(us)) {
    res += fmt::format("{}s", si);
  }
  return res;
}

/**
 * @brief From user's local time to system time. Uses timezone
 *
 * @param sv
 * @return std::chrono::system_clock::time_point
 */
std::chrono::system_clock::time_point TtlInfo::ParseStartTime(std::string_view sv) {
  try {
    // Midnight might be a problem...
    const auto now =
        std::chrono::year_month_day{std::chrono::floor<std::chrono::days>(std::chrono::system_clock::now())};
    const utils::DateParameters date{static_cast<int>(now.year()), static_cast<unsigned>(now.month()),
                                     static_cast<unsigned>(now.day())};
    auto [time, _] = utils::ParseLocalTimeParameters(sv);
    // LocalDateTime uses the user-defined timezone
    return std::chrono::system_clock::time_point{
        std::chrono::microseconds{utils::LocalDateTime(date, time).SysMicrosecondsSinceEpoch()}};
  } catch (const utils::temporal::InvalidArgumentException &e) {
    throw TtlException(e.what());
  }
}

/**
 *
 * @brief From system clock to user's local time. Uses timezone
 *
 * @param st
 * @return std::string
 */
std::string TtlInfo::StringifyStartTime(std::chrono::system_clock::time_point st) {
  const utils::LocalDateTime ldt(std::chrono::duration_cast<std::chrono::microseconds>(st.time_since_epoch()).count());
  auto epoch = std::chrono::microseconds{ldt.MicrosecondsSinceEpoch()};
  /* just consume and through away */
  GetPart<std::chrono::days>(epoch);
  /* what we are actually interested in */
  const auto h = GetPart<std::chrono::hours>(epoch);
  const auto m = GetPart<std::chrono::minutes>(epoch);
  const auto s = GetPart<std::chrono::seconds>(epoch);
  return fmt::format("{:02d}:{:02d}:{:02d}", h, m, s);
}

template bool TTL::Restore<dbms::DatabaseAccess>(dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context);
template void TTL::Setup_<dbms::DatabaseAccess>(dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context,
                                                bool should_run_edge_ttl);

}  // namespace memgraph::query::ttl

#endif  // MG_ENTERPRISE
