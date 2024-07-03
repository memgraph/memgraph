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

namespace memgraph::query::ttl {

template <typename TDbAccess>
void TTL::Execute(TtlInfo ttl_info, /*std::shared_ptr<QueryUserOrRole> owner,*/ TDbAccess db_acc,
                  InterpreterContext *interpreter_context) {
  auto ttl_locked = ttl_.Lock();
  if (!enabled_) {
    throw TtlException("TTL not enabled!");
  }
  if (ttl_locked->IsRunning()) {
    throw TtlException("TTL already running!");
  }

  auto interpreter =
      std::shared_ptr<query::Interpreter>(new Interpreter(interpreter_context, db_acc), [interpreter_context](auto *p) {
        p->Abort();
        interpreter_context->interpreters->erase(p);
        delete p;
      });

  // NOTE: We generate an empty user to avoid generating interpreter's fine grained access control and rely only
  // on the global auth_checker used in the stream itself
  // TODO: Fix auth inconsistency
  interpreter->SetUser(interpreter_context->auth_checker->GenQueryUser(std::nullopt, std::nullopt));
#ifdef MG_ENTERPRISE
  interpreter->OnChangeCB([](auto) { return false; });  // Disable database change
#endif
                                                        // register new interpreter into interpreter_context
  interpreter_context->interpreters->insert(interpreter.get());

  auto TTL = [interpreter = std::move(interpreter)]() {
    // TODO
    // auto ownername = owner->username();
    // auto rolename = owner->rolename();
    memgraph::query::DiscardValueResultStream result_stream;

    bool finished = false;
    while (!finished) {
      try {
        interpreter->BeginTransaction();
        auto prepare_result =
            interpreter->Prepare("MATCH (n:TTL) WHERE n.ttl < $now WITH n LIMIT $batch DETACH DELETE n;",
                                 [](auto) {
                                   UserParameters params;
                                   // Using microseconds to be aligned with timestamp() query, could just use seconds
                                   params.emplace("now", std::chrono::duration_cast<std::chrono::microseconds>(
                                                             std::chrono::system_clock::now().time_since_epoch())
                                                             .count());
                                   params.emplace("batch", 10000);
                                   return params;
                                 },
                                 {});
        // if (!owner->IsAuthorized(prepare_result.privileges, "", &up_to_date_policy)) {
        //   throw StreamsException{
        //       "Couldn't execute query '{}' for stream '{}' because the owner is not authorized to execute the "
        //       "query!",
        //       query, stream_name};
        // }
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
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
      } catch (const DatabaseContextRequiredException &e) {
        // No database; we are shutting down
        interpreter->Abort();
        break;
      }
      std::this_thread::yield();
    }
  };

  std::chrono::microseconds period = std::chrono::days(1);  // Default period is a day
  if (ttl_info.period) {
    period = *ttl_info.period;
  }
  std::optional<std::chrono::system_clock::time_point> start_time = std::nullopt;
  if (ttl_info.start_time) {
    start_time = std::chrono::system_clock::time_point{std::chrono::microseconds(*ttl_info.start_time)};
  }

  if (ttl_info)
    ttl_locked->Run(db_acc->name() + "-ttl", period, std::move(TTL), start_time);
  else  // one-shot
    TTL();
}

template void TTL::Execute<dbms::DatabaseAccess>(
    TtlInfo ttl_info,
    /*std::shared_ptr<QueryUserOrRole> owner,*/ dbms::DatabaseAccess db_acc, InterpreterContext *interpreter_context);

}  // namespace memgraph::query::ttl
