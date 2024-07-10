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

#ifdef MG_ENTERPRISE

namespace memgraph::query::ttl {

template <typename TDbAccess>
void TTL::Execute(TDbAccess db_acc, InterpreterContext *interpreter_context) {
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
  interpreter->SetUser(interpreter_context->auth_checker->GenQueryUser(std::nullopt, std::nullopt));
#ifdef MG_ENTERPRISE
  interpreter->OnChangeCB([](auto) { return false; });  // Disable database change
#endif
                                                        // register new interpreter into interpreter_context
  interpreter_context->interpreters->insert(interpreter.get());

  auto TTL = [interpreter = std::move(interpreter)]() {
    memgraph::query::DiscardValueResultStream result_stream;
    bool finished = false;
    while (!finished) {
      // Using microseconds to be aligned with timestamp() query, could just use seconds
      const auto now =
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch());
      try {
        interpreter->BeginTransaction();
        auto prepare_result =
            interpreter->Prepare("MATCH (n:TTL) WHERE n.ttl < $now WITH n LIMIT $batch DETACH DELETE n;",
                                 [now](auto) {
                                   UserParameters params;
                                   params.emplace("now", now.count());
                                   params.emplace("batch", 10000);
                                   return params;
                                 },
                                 {});
        const auto pull_res = interpreter->PullAll(&result_stream);
        finished = !pull_res.at("has_more").ValueBool() && std::invoke([&]() -> bool {
          // Empty set will not have a stats field (nothing happened, so nothing to report)
          const auto stats = pull_res.find("stats");
          return stats == pull_res.end() ||
                 // TODO: C++26 will handle transparent comparator with at()
                 stats->second.ValueMap().find("nodes-deleted")->second.ValueInt() == 0;
        });
        spdlog::trace("Commit TTL transaction");
        interpreter->CommitTransaction();
      } catch (const TransactionSerializationException &e) {
        spdlog::trace("TTL serialization error; Aborting and retrying...");
        interpreter->Abort();  // Retry later
<<<<<<< HEAD
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
=======
      } catch (const WriteQueryOnMainException & /* not used */) {
        // Preparation-time error, nothing to do
        interpreter->Abort();  // Retry later
        break;
      } catch (const WriteQueryOnReplicaException & /* not used */) {
        // Preparation-time error, nothing to do
        interpreter->Abort();  // Retry later
        break;
>>>>>>> 9f004e0e3 (TTL duraiblity and tests)
      } catch (const DatabaseContextRequiredException &e) {
        // No database; we are shutting down
        interpreter->Abort();
        break;
      }
      std::this_thread::yield();
    }
  };

  DMG_ASSERT(info_.period, "Period has to be defined for TTL");
  ttl_.Run(db_acc->name() + "-ttl", *info_.period, std::move(TTL), info_.start_time);
  Persist();
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
        Execute(db, interpreter_context);
      }
    }
  } catch (TtlException &e) {
    return fail(e.what());
  }
  return true;
}

template bool TTL::Restore<dbms::DatabaseAccess>(dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context);
template void TTL::Execute<dbms::DatabaseAccess>(dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context);

}  // namespace memgraph::query::ttl

#endif  // MG_ENTERPRISE
