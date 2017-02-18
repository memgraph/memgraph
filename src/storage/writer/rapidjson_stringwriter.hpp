#pragma once

#include <memory>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "storage/model/properties/properties.hpp"

class RapidJsonStringWriter {
 public:
  // TODO: clear up naming
  using sptr = std::shared_ptr<rapidjson::Writer<rapidjson::StringBuffer>>;

  RapidJsonStringWriter(const sptr& writer) : writer(writer) {}

  void handle(const std::string& key, Property& value, bool first) {
    writer->String(key.c_str());
    value.accept(*this);
  }

  void handle(Bool& b) { writer->String(b.value() ? "true" : "false"); }

  void handle(String& s) { writer->String(s.value.c_str()); }

  void handle(Int32& int32) {
    writer->String(std::to_string(int32.value).c_str());
  }

  void handle(Int64& int64) {
    writer->String(std::to_string(int64.value).c_str());
  }

  void handle(Float& f) { writer->String(std::to_string(f.value).c_str()); }

  void handle(Double& d) { writer->String(std::to_string(d.value).c_str()); }

 private:
  sptr writer;
};
