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

#include "storage/v2/ttl.hpp"

#include "storage/v2/access_type.hpp"
#include "storage/v2/inmemory/storage.hpp"

#ifdef MG_ENTERPRISE

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/bound.hpp"
#include "utils/temporal.hpp"

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

namespace memgraph::storage::ttl {

std::chrono::microseconds TtlInfo::ParsePeriod(std::string_view sv) {
  std::chrono::microseconds total{0};
  std::string_view remaining = sv;

  while (!remaining.empty()) {
    size_t pos = 0;
    while (pos < remaining.size() && std::isdigit(remaining[pos])) {
      ++pos;
    }
    if (pos == 0) {
      throw TtlException("Badly defined period. Use integers and 'd', 'h', 'm' and 's' to define it.");
    }

    const auto value = std::stoi(std::string(remaining.substr(0, pos)));
    remaining = remaining.substr(pos);

    if (remaining.empty()) {
      throw TtlException("Badly defined period. Use integers and 'd', 'h', 'm' and 's' to define it.");
    }

    const auto unit = remaining[0];
    remaining = remaining.substr(1);

    switch (unit) {
      case 'd':
        total += std::chrono::days{value};
        break;
      case 'h':
        total += std::chrono::hours{value};
        break;
      case 'm':
        total += std::chrono::minutes{value};
        break;
      case 's':
        total += std::chrono::seconds{value};
        break;
      default:
        throw TtlException("Badly defined period. Use integers and 'd', 'h', 'm' and 's' to define it.");
    }
  }

  return total;
}

std::string TtlInfo::StringifyPeriod(std::chrono::microseconds us) {
  auto remaining = us;
  std::string result;

  const auto days = GetPart<std::chrono::days>(remaining);
  if (days > 0) {
    result += fmt::format("{}d", days);
  }

  const auto hours = GetPart<std::chrono::hours>(remaining);
  if (hours > 0) {
    result += fmt::format("{}h", hours);
  }

  const auto minutes = GetPart<std::chrono::minutes>(remaining);
  if (minutes > 0) {
    result += fmt::format("{}m", minutes);
  }

  const auto seconds = GetPart<std::chrono::seconds>(remaining);
  if (seconds > 0) {
    result += fmt::format("{}s", seconds);
  }

  return result;
}

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

void TTL::SetInterval(std::optional<std::chrono::microseconds> period,
                      std::optional<std::chrono::system_clock::time_point> start_time) {
  if (!enabled_) {
    throw TtlException("TTL not enabled!");
  }
  if (!ttl_.IsRunning()) {
    throw TtlException("TTL not running!");
  }

  // Use provided period or default to 1 day
  auto actual_period = period.value_or(std::chrono::days(1));

  // Update the info_ with the new period and start_time
  info_.period = actual_period;
  info_.start_time = start_time;

  ttl_.SetInterval(actual_period, start_time);
}

void TTL::Configure(bool should_run_edge_ttl) {
  if (!enabled_) {
    throw TtlException("TTL not enabled!");
  }
  if (ttl_.IsRunning()) {
    throw TtlException("TTL already running!");
  }

  info_.should_run_edge_ttl = should_run_edge_ttl;

  if (info_.should_run_edge_ttl && (storage_ptr_->GetStorageMode() == StorageMode::ON_DISK_TRANSACTIONAL ||
                                    !storage_ptr_->config_.salient.items.properties_on_edges)) {
    spdlog::warn("Memgraph configuration doesn't support edge TTL. Edge TTL will be disabled.");
    info_.should_run_edge_ttl = false;
  }

  auto ttl_job = [this]() {
    // Check if we're a main instance - only main instances should run TTL
    if (!user_check_()) return;

    bool finished_vertex = false;
    bool finished_edge = !info_.should_run_edge_ttl;
    const auto now = std::chrono::system_clock::now();
    const auto now_us = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());

    spdlog::trace("Running TTL at {}",
                  std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());

    const auto ttl_label = storage_ptr_->NameToLabel("TTL");
    const auto ttl_property = storage_ptr_->NameToProperty("ttl");

    while (!finished_vertex || !finished_edge) {
      try {
        constexpr size_t batch_size = 10000;
        size_t n_deleted = 0;
        size_t n_edges_deleted = 0;

        // Create a new transaction for this batch
        // This ensures each batch is isolated and can be retried independently
        auto batch_accessor = storage_ptr_->Access(storage::StorageAccessType::WRITE);

        // Verify required indices exist and are ready for TTL operations
        // This ensures TTL can work efficiently using range-based filtering

        std::vector<PropertyPath> ttl_property_path = {ttl_property};
        bool const missing_lp_index = !batch_accessor->LabelPropertyIndexExists(ttl_label, ttl_property_path);
        bool const missing_edge_index = !batch_accessor->EdgePropertyIndexExists(ttl_property);

        if (missing_lp_index) {
          spdlog::warn(
              "TTL requires label+property index on :TTL(ttl) but it doesn't exist. Will create it automatically.");
          std::vector<PropertyPath> ttl_property_path = {storage_ptr_->NameToProperty("ttl")};

          if (storage_ptr_->GetStorageMode() == StorageMode::IN_MEMORY_TRANSACTIONAL) {
            auto *mem_storage = static_cast<storage::InMemoryStorage *>(storage_ptr_);
            mem_storage->GetAsyncIndexer().Enqueue(ttl_label, std::move(ttl_property_path));
          } else {
            batch_accessor.reset();
            batch_accessor = storage_ptr_->UniqueAccess();  // ATM no timeout/backoff
            (void)batch_accessor->CreateIndex(ttl_label, std::move(ttl_property_path));
            batch_accessor.reset();
            batch_accessor = storage_ptr_->Access(storage::StorageAccessType::WRITE);
          }
        }

        if (info_.should_run_edge_ttl && missing_edge_index) {
          spdlog::warn(
              "TTL requires edge property index on ttl property but it doesn't exist. Will create it automatically.");
          if (storage_ptr_->GetStorageMode() == StorageMode::IN_MEMORY_TRANSACTIONAL) {
            auto *mem_storage = static_cast<storage::InMemoryStorage *>(storage_ptr_);
            mem_storage->GetAsyncIndexer().Enqueue(ttl_property);
          } else {
            batch_accessor.reset();
            batch_accessor = storage_ptr_->UniqueAccess();  // ATM no timeout/backoff
            (void)batch_accessor->CreateGlobalEdgeIndex(ttl_property);
            batch_accessor.reset();
            batch_accessor = storage_ptr_->Access(storage::StorageAccessType::WRITE);
          }
        }

        bool const lp_index_ready = batch_accessor->LabelPropertyIndexReady(ttl_label, ttl_property_path);
        bool const edge_index_ready = info_.should_run_edge_ttl && batch_accessor->EdgePropertyIndexReady(ttl_property);

        // Process vertices with TTL label and ttl property using label+property index with range
        if (!finished_vertex && lp_index_ready) {
          // Use label+property index with range to efficiently find vertices where ttl < now
          // This is the most efficient approach as it uses the index to filter by property value
          std::vector<PropertyPath> ttl_property_path = {ttl_property};
          std::vector<PropertyValueRange> ttl_property_ranges = {
              PropertyValueRange::Bounded(std::nullopt, utils::MakeBoundExclusive(PropertyValue(now_us.count())))};
          auto vertices = batch_accessor->Vertices(ttl_label, ttl_property_path, ttl_property_ranges, View::NEW);
          std::vector<VertexAccessor> vertices_to_delete;
          vertices_to_delete.reserve(batch_size);

          for (const auto &vertex : vertices) {
            vertices_to_delete.push_back(vertex);
            if (vertices_to_delete.size() >= batch_size) break;  // Batch size limit
          }

          if (!vertices_to_delete.empty()) {
            // Need to convert to pointers to use the DetachDelete method
            std::vector<VertexAccessor *> vertices_to_delete_pointers;
            vertices_to_delete_pointers.reserve(vertices_to_delete.size());
            for (auto &vertex : vertices_to_delete) {
              vertices_to_delete_pointers.push_back(&vertex);
            }
            auto result = batch_accessor->DetachDelete(vertices_to_delete_pointers, {}, true);
            if (result.HasValue() && result.GetValue().has_value()) {
              n_deleted += result.GetValue()->first.size();
              n_edges_deleted += result.GetValue()->second.size();
            }
          }

          finished_vertex = vertices_to_delete.size() < batch_size;
        } else if (!finished_edge && edge_index_ready) {
          // Process edges with TTL property using range-based filtering
          // Use edge property index with range to efficiently find edges where ttl < now
          // This is much more efficient than using property index + checking each edge for the value
          auto edges = batch_accessor->Edges(ttl_property, std::nullopt,
                                             utils::MakeBoundExclusive(PropertyValue(now_us.count())), View::NEW);
          std::vector<EdgeAccessor> edges_to_delete;
          edges_to_delete.reserve(batch_size);

          for (const auto &edge : edges) {
            edges_to_delete.push_back(edge);
            if (edges_to_delete.size() >= batch_size) break;  // Batch size limit
          }

          if (!edges_to_delete.empty()) {
            // Need to convert to pointers to use the DetachDelete method
            std::vector<EdgeAccessor *> edges_to_delete_pointers;
            edges_to_delete_pointers.reserve(edges_to_delete.size());
            for (auto &edge : edges_to_delete) {
              edges_to_delete_pointers.push_back(&edge);
            }
            auto result = batch_accessor->DetachDelete({}, edges_to_delete_pointers, false);
            if (result.HasValue() && result.GetValue().has_value()) {
              n_edges_deleted += result.GetValue()->second.size();
            }
          }

          finished_edge = edges_to_delete.size() < batch_size;
        } else if (!lp_index_ready || (info_.should_run_edge_ttl && !edge_index_ready)) {
          spdlog::info("TTL indices not ready, skipping this run.");
        } else {
          DMG_ASSERT(false, "Unsupported TTL state.");
        }

        auto protector = storage_ptr_->make_database_protector();
        if (!protector) [[unlikely]] {
          // database has been dropped, we should aim to stop this worker ASAP
          return;
        }

        // Commit the transaction for this batch using the database protector
        auto commit_result = batch_accessor->PrepareForCommitPhase(CommitArgs::make_main(std::move(protector)));
        if (commit_result.HasError()) {
          // Transaction failed, will retry in next iteration
          // TODO: on sync replication error it should not continue since it commits
          continue;
        }
        // Telemetry
        memgraph::metrics::IncrementCounter(memgraph::metrics::DeletedNodes, n_deleted);
        memgraph::metrics::IncrementCounter(memgraph::metrics::DeletedEdges, n_edges_deleted);

      } catch (const std::exception &e) {
        spdlog::trace("TTL error; retrying later: {}", e.what());
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
      }

      std::this_thread::yield();
    }

    spdlog::trace("Finished TTL run from {}",
                  std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count());
  };

  // Starts the TTL job, but will not run until the period is set
  ttl_.Pause();
  ttl_.Run("storage-ttl", std::move(ttl_job));
}

}  // namespace memgraph::storage::ttl

#endif  // MG_ENTERPRISE
