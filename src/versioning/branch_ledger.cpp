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
#include <zlib.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <string_view>
#include <system_error>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include "utils/file.hpp"

namespace memgraph::versioning {

namespace {

constexpr std::string_view kMagic = "MGBL";

uint32_t Crc32(std::string_view data) {
  return static_cast<uint32_t>(
      ::crc32(::crc32(0L, Z_NULL, 0), reinterpret_cast<const Bytef *>(data.data()), static_cast<uInt>(data.size())));
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

  // Returns the close(2) result and disarms the destructor.
  int Close() {
    const int rc = ::close(fd_);
    fd_ = -1;
    return rc;
  }

 private:
  int fd_;
};

std::string Serialize(const LedgerContents &contents) {
  nlohmann::json branches = nlohmann::json::array();
  for (const auto &[name, info] : contents.branches) {
    nlohmann::json b{{"name", name},
                     {"number", info.number},
                     {"parent", info.parent},
                     {"fork_ts", info.fork_ts},
                     {"fork_durable_ts", info.fork_durable_ts}};
    if (info.description) b["description"] = *info.description;
    if (info.vertex_gid_floor) b["vertex_gid_floor"] = *info.vertex_gid_floor;
    if (info.edge_gid_floor) b["edge_gid_floor"] = *info.edge_gid_floor;
    branches.push_back(std::move(b));
  }
  const nlohmann::json root{{"next_number", contents.next_number}, {"branches", std::move(branches)}};
  return root.dump();
}

std::expected<LedgerContents, std::string> Deserialize(std::string_view payload) {
  try {
    const auto root = nlohmann::json::parse(payload);
    LedgerContents out;
    out.next_number = root.at("next_number").get<uint64_t>();
    for (const auto &b : root.at("branches")) {
      LedgerBranch lb;
      lb.name = b.at("name").get<std::string>();
      lb.info.number = b.at("number").get<uint64_t>();
      lb.info.parent = b.at("parent").get<std::string>();
      lb.info.fork_ts = b.at("fork_ts").get<uint64_t>();
      lb.info.fork_durable_ts = b.at("fork_durable_ts").get<uint64_t>();
      if (auto it = b.find("description"); it != b.end()) lb.info.description = it->get<std::string>();
      if (auto it = b.find("vertex_gid_floor"); it != b.end()) lb.info.vertex_gid_floor = it->get<uint64_t>();
      if (auto it = b.find("edge_gid_floor"); it != b.end()) lb.info.edge_gid_floor = it->get<uint64_t>();
      if (lb.info.number >= out.next_number) {
        return std::unexpected(
            fmt::format("branch '{}' has number {} >= next_number {}", lb.name, lb.info.number, out.next_number));
      }
      out.branches.push_back(std::move(lb));
    }
    return out;
  } catch (const nlohmann::json::exception &e) {
    return std::unexpected(fmt::format("malformed payload: {}", e.what()));
  }
}

}  // namespace

BranchLedger::BranchLedger(std::filesystem::path file) : file_(std::move(file)) {}

std::expected<void, std::string> BranchLedger::Write(const LedgerContents &contents) const {
  const auto dir = file_.parent_path();
  if (!dir.empty() && !utils::EnsureDir(dir)) {
    return std::unexpected(fmt::format("Branch ledger: cannot create directory '{}'.", dir.string()));
  }

  const std::string payload = Serialize(contents);
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
  if (::fsync(fd.get()) != 0) return fail("Branch ledger: " + Errno("cannot fsync", tmp));
  if (fd.Close() != 0) return fail("Branch ledger: " + Errno("cannot close", tmp));

  if (::rename(tmp.c_str(), file_.c_str()) != 0)
    return fail("Branch ledger: " + Errno("cannot rename into place", tmp));

  // The rename is only durable once the directory entry is.
  const auto sync_dir = dir.empty() ? std::filesystem::path{"."} : dir;
  Fd dir_fd{::open(sync_dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
  if (dir_fd.get() < 0) return std::unexpected("Branch ledger: " + Errno("cannot open directory", sync_dir));
  if (::fsync(dir_fd.get()) != 0) return std::unexpected("Branch ledger: " + Errno("cannot fsync directory", sync_dir));
  return {};
}

std::expected<std::optional<LedgerContents>, std::string> BranchLedger::Read() const {
  std::error_code ec;
  if (!std::filesystem::exists(file_, ec)) return std::optional<LedgerContents>{};

  std::ifstream in(file_, std::ios::binary);
  if (!in) return std::unexpected(fmt::format("Branch ledger: cannot open '{}'.", file_.string()));
  const std::string data{std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};

  const auto corrupt = [&](std::string_view why) {
    return std::unexpected(fmt::format("Branch ledger '{}' is corrupt: {}.", file_.string(), why));
  };

  const auto nl = data.find('\n');
  if (nl == std::string::npos) return corrupt("missing header");
  const std::string header = data.substr(0, nl);
  const std::string_view payload = std::string_view{data}.substr(nl + 1);

  char magic[8]{};
  unsigned version = 0;
  unsigned long long size = 0;
  unsigned crc = 0;
  if (std::sscanf(header.c_str(), "%7s %u %llu %x", magic, &version, &size, &crc) != 4 || magic != kMagic) {
    return corrupt("bad header");
  }
  if (version != kVersion) return corrupt(fmt::format("unsupported version {}", version));
  if (size != payload.size()) {
    return corrupt(fmt::format("payload size {} does not match header size {}", payload.size(), size));
  }
  if (Crc32(payload) != crc) return corrupt("checksum mismatch");

  auto parsed = Deserialize(payload);
  if (!parsed) return corrupt(parsed.error());
  return std::optional<LedgerContents>{std::move(*parsed)};
}

}  // namespace memgraph::versioning
