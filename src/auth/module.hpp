// Copyright 2024 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
//
//

/// @file
#pragma once

#include <filesystem>
#include <map>
#include <mutex>
#include <string>

#include <json/json.hpp>

namespace memgraph::auth {
struct TargetArguments {
  std::filesystem::path module_executable_path;
  int pipe_to_module{-1};
  int pipe_from_module{-1};
};

/// Wrapper around the module executable.
class Module final {
 private:
  const int kStackSizeBytes = 262144;

 public:
  explicit Module(const std::filesystem::path &module_executable_path);

  Module(const Module &) = delete;
  Module(Module &&) = delete;
  Module &operator=(const Module &) = delete;
  Module &operator=(Module &&) = delete;

  /// Call the function in the module with the specified parameters and return
  /// the response.
  ///
  /// @param parameters dict used to call the module function
  /// @param timeout_millisec timeout in ms used for communication with the
  ///                         module
  /// @return dict retuned by module function
  nlohmann::json Call(const nlohmann::json &params, int timeout_millisec);

  /// This function returns a boolean value indicating whether the module has a
  /// specified executable path and can thus be used.
  ///
  /// @return boolean indicating whether the module can be used
  bool IsUsed() const;

  ~Module();

 private:
  bool Startup();
  void Shutdown();

  std::filesystem::path module_executable_path_;
  std::mutex lock_;
  pid_t pid_{-1};
  int status_{0};
  // The stack used for the `clone` system call must be heap allocated.
  std::unique_ptr<uint8_t[]> stack_{new uint8_t[kStackSizeBytes]};
  // The target arguments passed to the new process must be heap allocated.
  std::unique_ptr<TargetArguments> target_arguments_{new TargetArguments()};
  int pipe_to_module_[2] = {-1, -1};
  int pipe_from_module_[2] = {-1, -1};
};

}  // namespace memgraph::auth
