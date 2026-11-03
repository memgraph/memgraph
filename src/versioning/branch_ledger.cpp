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

#include "versioning/branch_ledger.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <charconv>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <ios>
#include <iterator>
#include <limits>
#include <ranges>
#include <set>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <vector>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "utils/crc_accumulator.hpp"
#include "utils/file.hpp"
#include "versioning/name.hpp"

namespace memgraph::versioning {

namespace {

constexpr std::string_view kMagic = "MGBL";

uint32_t Crc32(std::string_view data) {
  utils::CrcAccumulator crc;
  crc.Update(reinterpret_cast<const unsigned char *>(data.data()), static_cast<uint32_t>(data.size()));
  return crc.Value();
}

std::string Errno(std::string_view what, const std::filesystem::path &path) {
  return fmt::format("{} '{}': {}", what, path.string(), std::strerror(errno));
}

class Fd {
 public:
  explicit Fd(int fd) : fd_(fd) {}

  Fd(const Fd &) = delete;
  Fd &operator=(const Fd &) = delete;

  ~Fd() {
    if (fd_ >= 0) ::close(fd_);
  }

  int get() const { return fd_; }

  int Close() {
    const int rc = ::close(fd_);
    fd_ = -1;
    return rc;
  }

 private:
  int fd_;
};

int FsyncRetrying(int fd) {
  int rc = 0;
  do {
    rc = ::fsync(fd);
  } while (rc != 0 && errno == EINTR);
  return rc;
}

std::expected<void, std::string> SyncDirectory(const std::filesystem::path &dir) {
  Fd const fd{::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
  if (fd.get() < 0) return std::unexpected(Errno("cannot open directory", dir));
  // The message is built before `fd` closes, so errno is still fsync's.
  if (FsyncRetrying(fd.get()) != 0) return std::unexpected(Errno("cannot fsync directory", dir));
  return {};
}

// What Read accepts and Write persists. A parent need only be a valid name, not a listed branch.
std::optional<std::string> Validate(const LedgerContents &contents) {
  if (contents.next_number < 2) return fmt::format("next_number {} is below 2", contents.next_number);
  std::set<std::string_view> names;
  std::set<uint64_t> numbers;
  for (const auto &[name, info] : contents.branches) {
    if (auto invalid = ValidateBranchName(name)) return fmt::format("branch '{}': {}", name, *invalid);
    if (info.parent != "main") {
      if (auto invalid = ValidateBranchName(info.parent)) {
        return fmt::format("branch '{}' has parent '{}': {}", name, info.parent, *invalid);
      }
    }
    if (info.number < 2 || info.number >= contents.next_number) {
      return fmt::format(
          "branch '{}' has number {} outside [2, next_number {})", name, info.number, contents.next_number);
    }
    if (info.vertex_gid_floor && info.vertex_gid_ceiling && *info.vertex_gid_ceiling < *info.vertex_gid_floor) {
      return fmt::format("branch '{}' has a vertex gid ceiling below its floor", name);
    }
    if (info.edge_gid_floor && info.edge_gid_ceiling && *info.edge_gid_ceiling < *info.edge_gid_floor) {
      return fmt::format("branch '{}' has an edge gid ceiling below its floor", name);
    }
    if (!names.insert(name).second) return fmt::format("branch '{}' is listed twice", name);
    if (!numbers.insert(info.number).second) return fmt::format("branch number {} is used twice", info.number);
  }
  return std::nullopt;
}

// get<uint64_t>() would silently convert a negative or fractional number.
uint64_t Unsigned(const nlohmann::json &value, const char *key) {
  if (!value.is_number_unsigned()) throw std::invalid_argument(fmt::format("'{}' is not an unsigned integer", key));
  return value.get<uint64_t>();
}

std::optional<uint64_t> OptionalUnsigned(const nlohmann::json &object, const char *key) {
  const auto it = object.find(key);
  if (it == object.end()) return std::nullopt;
  return Unsigned(*it, key);
}

std::string Serialize(const LedgerContents &contents) {
  nlohmann::json branches = nlohmann::json::array();
  for (const auto &[name, info] : contents.branches) {
    nlohmann::json b{{"name", name},
                     {"number", info.number},
                     {"parent", info.parent},
                     // Persist the recorded boundary, not the runtime pin: they differ after startup restore.
                     {"fork_ts", info.recorded_fork_ts}};
    if (info.vertex_gid_floor) b["vertex_gid_floor"] = *info.vertex_gid_floor;
    if (info.edge_gid_floor) b["edge_gid_floor"] = *info.edge_gid_floor;
    if (info.vertex_gid_ceiling) b["vertex_gid_ceiling"] = *info.vertex_gid_ceiling;
    if (info.edge_gid_ceiling) b["edge_gid_ceiling"] = *info.edge_gid_ceiling;
    if (info.created_at_us) b["created_at_us"] = *info.created_at_us;
    if (info.fork_counts) {
      b["fork_vertex_count"] = info.fork_counts->vertices;
      b["fork_edge_count"] = info.fork_counts->edges;
    }
    branches.push_back(std::move(b));
  }
  const nlohmann::json root{{"next_number", contents.next_number}, {"branches", std::move(branches)}};
  return root.dump();
}

std::expected<LedgerContents, std::string> Deserialize(std::string_view payload) {
  LedgerContents out;
  try {
    const auto root = nlohmann::json::parse(payload);
    out.next_number = Unsigned(root.at("next_number"), "next_number");
    const auto &branches = root.at("branches");
    if (!branches.is_array()) throw std::invalid_argument("'branches' is not an array");
    for (const auto &b : branches) {
      out.branches.push_back({.name = b.at("name").get<std::string>(),
                              .info = {.number = Unsigned(b.at("number"), "number"),
                                       .parent = b.at("parent").get<std::string>(),
                                       .fork_ts = Unsigned(b.at("fork_ts"), "fork_ts"),
                                       .vertex_gid_floor = OptionalUnsigned(b, "vertex_gid_floor"),
                                       .edge_gid_floor = OptionalUnsigned(b, "edge_gid_floor"),
                                       .vertex_gid_ceiling = OptionalUnsigned(b, "vertex_gid_ceiling"),
                                       .edge_gid_ceiling = OptionalUnsigned(b, "edge_gid_ceiling"),
                                       .created_at_us = OptionalUnsigned(b, "created_at_us")}});
      if (b.contains("fork_vertex_count") && b.contains("fork_edge_count")) {
        out.branches.back().info.fork_counts = ObjectCounts{.vertices = Unsigned(b.at("fork_vertex_count"), "fork_vertex_count"),
                                                            .edges = Unsigned(b.at("fork_edge_count"), "fork_edge_count")};
      }
    }
  } catch (const nlohmann::json::exception &e) {
    return std::unexpected(fmt::format("malformed payload: {}", e.what()));
  } catch (const std::invalid_argument &e) {
    return std::unexpected(fmt::format("malformed payload: {}", e.what()));
  }
  if (auto invalid = Validate(out)) return std::unexpected(std::move(*invalid));
  return out;
}

template <typename T>
std::optional<T> ParseUnsigned(std::string_view token, int base = 10) {
  T value{};
  const auto *const begin = std::to_address(token.begin());
  const auto *const end = std::to_address(token.end());
  const auto [ptr, ec] = std::from_chars(begin, end, value, base);
  if (ec != std::errc{} || ptr != end) return std::nullopt;
  return value;
}

}  // namespace

BranchLedger::BranchLedger(std::filesystem::path file) : file_(std::move(file)) {}

std::expected<void, std::string> BranchLedger::Write(const LedgerContents &contents) const {
  // Also keeps dump() from throwing: valid names are ASCII.
  if (auto invalid = Validate(contents)) {
    return std::unexpected(fmt::format("Branch ledger: refusing to write an invalid registry: {}.", *invalid));
  }

  const auto dir = file_.parent_path();
  if (!dir.empty()) {
    if (!utils::EnsureDir(dir)) {
      return std::unexpected(fmt::format("Branch ledger: cannot create directory '{}'.", dir.string()));
    }
    // The directory may have just been created; its own entry must be durable before the ledger is renamed into it.
    const auto parent = dir.parent_path();
    if (auto synced = SyncDirectory(parent.empty() ? std::filesystem::path{"."} : parent); !synced) {
      return std::unexpected("Branch ledger: " + synced.error());
    }
  }

  const std::string payload = Serialize(contents);
  // The header stores the payload size and CRC as uint32; refuse instead of silently truncating.
  if (payload.size() > std::numeric_limits<uint32_t>::max()) {
    return std::unexpected(fmt::format("Branch ledger: payload of {} bytes is too large.", payload.size()));
  }
  const std::string data =
      fmt::format("{} {} {} {:08x}\n{}", kMagic, kVersion, payload.size(), Crc32(payload), payload);

  auto tmp = file_;
  tmp += ".tmp";

  Fd fd{::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644)};
  if (fd.get() < 0) return std::unexpected("Branch ledger: " + Errno("cannot open", tmp));

  const auto fail = [&](std::string msg) -> std::expected<void, std::string> {
    std::error_code ec;
    std::filesystem::remove(tmp, ec);
    return std::unexpected(std::move(msg));
  };

  size_t written = 0;
  while (written < data.size()) {
    const ssize_t n = ::write(fd.get(), data.data() + written, data.size() - written);
    if (n < 0) {
      if (errno == EINTR) continue;
      return fail("Branch ledger: " + Errno("cannot write", tmp));
    }
    written += static_cast<size_t>(n);
  }
  if (FsyncRetrying(fd.get()) != 0) return fail("Branch ledger: " + Errno("cannot fsync", tmp));
  if (fd.Close() != 0) return fail("Branch ledger: " + Errno("cannot close", tmp));

  if (::rename(tmp.c_str(), file_.c_str()) != 0)
    return fail("Branch ledger: " + Errno("cannot rename into place", tmp));

  // The new ledger is already in place, so the write counts as done; a failed directory fsync only weakens durability
  // of the rename, and reporting it would make the caller roll back state the disk no longer matches.
  const auto sync_dir = dir.empty() ? std::filesystem::path{"."} : dir;
  if (auto synced = SyncDirectory(sync_dir); !synced) {
    spdlog::warn("Branch ledger: directory fsync after rename failed: {}", synced.error());
  }
  return {};
}

std::expected<std::optional<LedgerContents>, std::string> BranchLedger::Read() const {
  std::error_code ec;
  const bool present = std::filesystem::exists(file_, ec);
  if (ec) return std::unexpected(fmt::format("Branch ledger: cannot stat '{}': {}.", file_.string(), ec.message()));
  if (!present) return std::optional<LedgerContents>{};
  if (!std::filesystem::is_regular_file(file_, ec)) {
    return std::unexpected(fmt::format("Branch ledger '{}' is not a regular file.", file_.string()));
  }

  std::ifstream in(file_, std::ios::binary);
  if (!in) return std::unexpected(fmt::format("Branch ledger: cannot open '{}'.", file_.string()));
  std::string data;
  try {
    data.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
  } catch (const std::ios_base::failure &) {
    return std::unexpected(fmt::format("Branch ledger: cannot read '{}'.", file_.string()));
  }

  const auto corrupt = [&](std::string_view why) {
    return std::unexpected(fmt::format("Branch ledger '{}' is corrupt: {}.", file_.string(), why));
  };

  const auto nl = data.find('\n');
  if (nl == std::string::npos) return corrupt("missing header");
  const std::string header = data.substr(0, nl);
  const std::string_view payload = std::string_view{data}.substr(nl + 1);

  std::vector<std::string_view> tokens;
  for (const auto token : std::views::split(std::string_view{header}, ' ')) tokens.emplace_back(token);
  if (tokens.size() != 4 || tokens[0] != kMagic) return corrupt("bad header");
  const auto version = ParseUnsigned<uint32_t>(tokens[1]);
  const auto size = ParseUnsigned<uint64_t>(tokens[2]);
  const auto crc = ParseUnsigned<uint32_t>(tokens[3], 16);
  if (!version || !size || !crc) return corrupt("bad header");
  if (*version != kVersion) return corrupt(fmt::format("unsupported version {}", *version));
  if (*size != payload.size()) {
    return corrupt(fmt::format("payload size {} does not match header size {}", payload.size(), *size));
  }
  if (Crc32(payload) != *crc) return corrupt("checksum mismatch");

  auto parsed = Deserialize(payload);
  if (!parsed) return corrupt(parsed.error());
  return std::optional<LedgerContents>{std::move(*parsed)};
}

}  // namespace memgraph::versioning
