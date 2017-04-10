#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "query/typed_value.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

class PrintRecordStream {
 private:
  std::ostream &stream;

 public:
  PrintRecordStream(std::ostream &stream) : stream(stream) {}

  // TODO: all these functions should pretty print their data
  void Header(const std::vector<std::string> &fields) {
    stream << "Header\n";
  }

  void Result(std::vector<query::TypedValue> &values) {
    stream << "Result\n";
  }

  void Summary(const std::map<std::string, query::TypedValue> &summary) {
    stream << "Summary\n";
  }
};
