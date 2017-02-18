#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "utils/exceptions/not_yet_implemented.hpp"

class PrintRecordStream {
 private:
  std::ostream &stream;

 public:
  PrintRecordStream(std::ostream &stream) : stream(stream) {}

  void write_success() { stream << "SUCCESS\n"; }

  void write_success_empty() { stream << "SUCCESS EMPTY\n"; }

  void write_ignored() { stream << "IGNORED\n"; }

  void write_empty_fields() { stream << "EMPTY FIELDS\n"; }

  void write_fields(const std::vector<std::string> &fields) {
    stream << "FIELDS:";
    for (auto &field : fields) {
      stream << " " << field;
    }
    stream << '\n';
  }

  void write_field(const std::string &field) {
    stream << "Field: " << field << '\n';
  }

  void write_list_header(size_t size) { stream << "List: " << size << '\n'; }

  void write_record() { stream << "Record\n"; }

  void write_meta(const std::string &type) {
    stream << "Meta: " << type << std::endl;
  }

  void write_failure(const std::map<std::string, std::string> &data) {
    throw NotYetImplemented();
  }

  void write_count(const size_t count) { throw NotYetImplemented(); }

  void write(const VertexAccessor &vertex) { throw NotYetImplemented(); }

  void write_vertex_record(const VertexAccessor &va) {
    throw NotYetImplemented();
  }

  void write(const EdgeAccessor &edge) { throw NotYetImplemented(); }

  void write_edge_record(const EdgeAccessor &ea) { throw NotYetImplemented(); }

  void send() { throw NotYetImplemented(); }

  void chunk() { throw NotYetImplemented(); }
};
