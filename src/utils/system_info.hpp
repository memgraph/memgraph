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

#pragma once

#include <cstdint>
#include <filesystem>
#include <nlohmann/json_fwd.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace memgraph::utils {

// This is a bit imprecise but it is important that we can be certain about whether we are running in K8s or not
// For Docker, .dockerenv file apparently doesn't exist always so it could lead us into wrong direction that people
// aren't actually using that much Docker while in fact they do
enum class RuntimeEnv : uint8_t { KUBERNETES, NO_KUBERNETES };

struct MemoryInfo {
  uint64_t memory;
  uint64_t swap;
};

struct CPUInfo {
  std::string cpu_model;
  uint64_t cpu_count;
  uint8_t microarch_level;
};

std::string GetMachineId();

inline constexpr auto kInstallIdFileName = ".memgraph_install_id";

/// Returns the UUID identifying this Memgraph installation (i.e. this data directory). It's stored in
/// `<data_directory>/.memgraph_install_id` and reused on every start. A new one is generated if the file is missing or
/// doesn't hold a valid UUID. If the file can't be written, the newly generated id is still returned, but it'll only
/// last for the current run. Call it only while holding the data directory lock.
std::string GetOrCreateInstallId(const std::filesystem::path &data_directory);

/// Reads the hostname from /etc/hostname. Returns "UNKNOWN" if it can't be read.
std::string GetHostname();

MemoryInfo GetMemoryInfo();

CPUInfo GetCPUInfo(const std::string &machine);

uint8_t DetectX86LevelFromFlags(const std::unordered_set<std::string> &flags);

uint8_t DetectArmArchitectureLevel(const std::vector<std::string> &cpu_data);

bool HasCPUFlag(const std::unordered_set<std::string> &flags, const std::string &flag);

std::unordered_set<std::string> ExtractCPUFlags(const std::vector<std::string> &cpu_data);

std::string ExtractArmCPUVariant(const std::vector<std::string> &cpu_data);

RuntimeEnv DetectRuntimeEnv();

inline constexpr auto kKubernetesServiceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";

/// Extracts the pod UID from a Kubernetes service account token (a JWT). Bound service account tokens, which are
/// projected into pods by default, carry it in the `kubernetes.io.pod.uid` claim. Only the payload is decoded; the
/// signature isn't verified, so never use the result for anything security-related. Returns nullopt if the token can't
/// be parsed, doesn't have the claim (e.g. a legacy secret-based token), or the claim isn't a UUID.
std::optional<std::string> ExtractPodUidFromServiceAccountToken(std::string_view token);

/// Reads the service account token mounted into the pod and returns the pod UID from it. Returns nullopt if the token
/// isn't mounted (e.g. not running in Kubernetes, or `automountServiceAccountToken: false`) or doesn't carry the UID.
std::optional<std::string> GetKubernetesPodUid(
    const std::filesystem::path &token_path = kKubernetesServiceAccountTokenPath);

/**
 * Returns the number of hardware threads available, with a guaranteed
 * non-zero result. Tries std::thread::hardware_concurrency() first,
 * falls back to parsing /proc/cpuinfo, and finally uses the numeric
 * fallback if neither source succeeds.
 *
 * @param fallback Value to use as a last resort (default: 2).
 * @return Number of hardware threads, always > 0.
 */
unsigned GetSafeHardwareConcurrency(unsigned fallback = 2);

/**
 * This function return a dictionary containing some basic system information
 * (eg. operating system name, cpu information, memory information, etc.).
 */
nlohmann::json GetSystemInfo();

}  // namespace memgraph::utils
