// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <string>

// https://stackoverflow.com/questions/142508/how-do-i-check-os-with-a-preprocessor-directive
// TODO(gitbuda): Deal with the flags in both: OS case and ASAN case.

namespace memgraph::utils {

#if defined(__linux__) || defined(__APPLE__)
#include <dlfcn.h>

class LinuxDLHandle {
 public:
  LinuxDLHandle(const std::string &shared_library, int mode) : handle_{dlopen(shared_library.c_str(), mode)} {}
  LinuxDLHandle(const LinuxDLHandle &) = delete;
  LinuxDLHandle(LinuxDLHandle &&) = delete;
  LinuxDLHandle operator=(const LinuxDLHandle &) = delete;
  LinuxDLHandle operator=(LinuxDLHandle &&) = delete;

  ~LinuxDLHandle() {
    if (handle_) {
      dlclose(handle_);
    }
  }

 private:
  void *handle_;
};
#endif

#ifdef _WIN32
#include <windows.h>
class WindowsDLLHandle {};
#endif

}  // namespace memgraph::utils
