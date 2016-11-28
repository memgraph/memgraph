#pragma once

#include <algorithm>
#include <exception>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include "utils/option.hpp"

namespace {

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-function"

class ProgramArgumentException : public std::exception {
 public:
  virtual const char *what() const throw() {
    return "Invalid Command Line Arguments";
  }
} arg_exception;

class ProgramArguments {
 private:
  std::map<std::string, std::vector<std::string>> arguments_;
  std::vector<std::string> required_arguments_;
  std::mutex mutex_;

  bool is_flag(const std::string &arg) { return arg[0] == '-'; }

  bool is_valid() {
    for (const auto &arg : required_arguments_)
      for (const auto &a : arguments_)
        if (!arguments_.count(arg)) return false;
    return true;
  }

 protected:
  ProgramArguments() {}
  ~ProgramArguments() {}

 public:
  class Argument {
   private:
    std::string arg_;

   public:
    Argument(const std::string &arg) : arg_(arg) {}
    std::string GetString() { return arg_; }
    int GetInteger() { return std::atoi(arg_.c_str()); };
    float GetFloat() { return std::atof(arg_.c_str()); };
    long GetLong() { return std::atol(arg_.c_str()); }
  };

  static ProgramArguments &instance() {
    static ProgramArguments instance_;
    return instance_;
  }

  ProgramArguments(ProgramArguments const &) = delete;
  ProgramArguments(ProgramArguments &&) = delete;
  ProgramArguments &operator=(ProgramArguments const &) = delete;
  ProgramArguments &operator=(ProgramArguments &&) = delete;

  void register_args(int argc, char *argv[]) {
    std::lock_guard<std::mutex> lock(mutex_);

    for (int i = 1; i < argc; i++) {
      std::string flag(*(argv + i));
      if (is_flag(flag)) {
        arguments_[flag] = {};
        while (i < argc - 1) {
          i++;
          std::string arg(*(argv + i));
          if (!is_flag(arg))
            arguments_[flag].push_back(arg);
          else {
            i--;
            break;
          }
        }
      }
    }

    if (required_arguments_.empty()) return;

    if (!is_valid()) throw arg_exception;
  }

  void register_required_args(std::vector<std::string> args) {
    required_arguments_ = args;

    if (arguments_.empty()) return;

    if (!is_valid()) throw arg_exception;
  }

  bool contains_flag(const std::string &flag) {
    return arguments_.count(flag) > 0;
  }

  auto get_arg(const std::string &flag, const std::string &default_value) {
    if (contains_flag(flag)) return Argument(arguments_[flag][0]);
    return Argument(default_value);
  }

  auto get_arg_list(const std::string &flag,
                    const std::vector<std::string> &default_value) {
    std::vector<Argument> ret;
    if (contains_flag(flag)) {
      for (const auto &arg : arguments_[flag]) ret.emplace_back(arg);
    } else
      for (const auto &arg : default_value) ret.emplace_back(arg);
    return ret;
  }

  void clear() {
    arguments_.clear();
    required_arguments_.clear();
  }
};

auto all_arguments(int argc, char *argv[]) {
  return std::vector<std::string>(argv + 1, argv + argc);
}

bool contains_argument(const std::vector<std::string> &all,
                       const std::string &flag) {
  return std::find(all.begin(), all.end(), flag) != all.end();
}

// just returns argument value
auto get_argument(const std::vector<std::string> &all, const std::string &flag,
                  const std::string &default_value) {
  auto it = std::find(all.begin(), all.end(), flag);

  if (it == all.end()) return default_value;

  return all[std::distance(all.begin(), it) + 1];
}

Option<std::string> take_argument(std::vector<std::string> &all,
                                  const std::string &flag) {
  auto it = std::find(all.begin(), all.end(), flag);

  if (it == all.end()) return make_option<std::string>();

  auto s = std::string(all[std::distance(all.begin(), it) + 1]);
  it++;
  it++;
  all.erase(std::find(all.begin(), all.end(), flag), it);

  return make_option<std::string>(std::move(s));
}

#pragma clang diagnostic pop
}
