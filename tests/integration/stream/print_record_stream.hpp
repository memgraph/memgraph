#pragma once

#include <iostream>
#include <map>
#include <string>
#include <vector>

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
  }

  void write_count(const size_t count) { }
};
