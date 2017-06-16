#pragma once

#include "logging/log.hpp"
#include "logging/streams/format.hpp"

class File : public Log::Stream {
 public:
  File(const std::string &filename) : file_(filename) {}
  void emit(const Log::Record &record) override {
    file_ << logging::Formatter::format(logging::format::out, record);
    file_.flush();
  }

 private:
  std::ofstream file_;
};

