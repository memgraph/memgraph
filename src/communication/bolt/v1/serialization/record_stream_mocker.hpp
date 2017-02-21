#pragma once

#include <deque>
#include <map>
#include <stack>

#include "support/Any.h"

#include "logging/default.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

namespace bolt {

/**
 * this class should be used to mock RecordStream.
 */
class RecordStreamMocker {
 public:
  RecordStreamMocker() { logger_ = logging::log->logger("Record Stream"); }

  ~RecordStreamMocker() = default;

  // TODO: create apstract methods that are not bolt specific ---------------
  void write_success() { logger_.trace("write_success"); }
  void write_success_empty() { logger_.trace("write_success_empty"); }
  void write_ignored() { logger_.trace("write_ignored"); }

  void write_empty_fields() {}

  void write_fields(const std::vector<std::string> &fields) {
    messages_.push_back(std::make_pair("fields", Message(fields)));
  }

  void write_field(const std::string &field) {
    messages_.push_back(std::make_pair("fields", Message({field})));
  }

  void write_list_header(size_t size) {
    messages_.back().second.write_list(size);
  }

  void write_record() {}

  // writes metadata at the end of the message
  // TODO: write whole implementation (currently, only type is supported)
  // { "stats": { "nodes created": 1, "properties set": 1},
  //   "type": "r" | "rw" | ...
  void write_meta(const std::string &type) {
    messages_.push_back(std::make_pair("meta", Message({type})));
  }

  void write_failure(const std::map<std::string, std::string> &data) {}

  void write_count(const size_t count) {
    messages_.back().second.write(antlrcpp::Any(count));
  }
  void write(const VertexAccessor &vertex) {
    messages_.back().second.write(antlrcpp::Any(vertex));
  }
  void write(const EdgeAccessor &edge) {
    messages_.back().second.write(antlrcpp::Any(edge));
  }
  void write(const TypedValue &value) {
    messages_.back().second.write(antlrcpp::Any(value));
  }

  void write_edge_record(const EdgeAccessor &ea) { write(ea); }
  void write_vertex_record(const VertexAccessor &va) { write(va); }

  // Returns number of columns associated with 'message_name', 0 if no such
  // message exists.
  size_t count_message_columns(const std::string &message_name) {
    for (auto x : messages_)
      if (x.first == message_name) return x.second.count_columns();
    return 0;
  }

  // Returns a vector of items inside column 'index' inside Message
  // 'message_name'. Empty vector is returned if no such Message exists.
  std::vector<antlrcpp::Any> get_message_column(const std::string &message_name,
                                                int index) {
    for (auto x : messages_)
      if (x.first == message_name) return x.second.get_column(index);
    return {};
  }

  // Returns a vector of column names (headers) for message 'message_name'.
  // Empty vector is returned if no such message exists.
  std::vector<std::string> get_message_headers(
      const std::string &message_name) {
    for (auto x : messages_)
      if (x.first == message_name) return x.second.get_headers();
    return {};
  }

 protected:
  Logger logger_;

 private:
  class Message {
   public:
    Message(const std::vector<std::string> &header_labels)
        : header_labels_(header_labels) {}
    std::vector<std::string> get_headers() const { return header_labels_; }

    // Returns vector of items belonging to column 'column'.
    std::vector<antlrcpp::Any> get_column(int column) {
      size_t index = 0;
      std::vector<antlrcpp::Any> ret;
      for (int i = 0;; ++i) {
        if (index == values_.size()) break;
        auto item = read(index);
        if (i % header_labels_.size() == column) ret.push_back(item);
      }
      return ret;
    }

    size_t count_columns() const { return header_labels_.size(); }

    enum TYPE { LIST, MAP, SINGLETON };
    antlrcpp::Any read(size_t &index) {
      if (index == values_.size()) return false;
      std::tuple<enum TYPE, size_t, antlrcpp::Any> curr = values_[index++];
      if (std::get<0>(curr) == MAP) {
        throw "Not implemented.";
      }
      if (std::get<0>(curr) == LIST) {
        std::vector<antlrcpp::Any> ret;
        for (int i = 0; i < std::get<1>(curr); ++i) ret.push_back(read(index));
        return ret;
      }
      if (std::get<0>(curr) == SINGLETON) {
        return std::get<2>(curr);
      }
      return false;
    }

    void write_list(size_t size) {
      values_.push_back(std::make_tuple(
          TYPE::LIST, size, antlrcpp::Any(std::vector<antlrcpp::Any>())));
    }

    void write(const antlrcpp::Any &value) {
      values_.push_back(std::make_tuple(TYPE::SINGLETON, (size_t)1, value));
    }

    std::vector<std::string> header_labels_;
    std::vector<antlrcpp::Any> values_;
  };

  std::vector<std::pair<std::string, Message>> messages_;
};
}
