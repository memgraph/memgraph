#pragma once

#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include <experimental/filesystem>
#include <utils/exceptions/basic_exception.hpp>
#include "fmt/format.h"
#include "logging/default.hpp"
#include "utils/exceptions/stacktrace_exception.hpp"
namespace fs = std::experimental::filesystem;
#include "utils/file.hpp"
#include "utils/string/file.hpp"
#include "utils/string/trim.hpp"
#include "utils/types/byte.hpp"

using std::cout;
using std::endl;

// this is a nice way how to avoid multiple definition problem with
// headers because it will create a unique namespace for each compilation unit
// http://stackoverflow.com/questions/2727582/multiple-definition-in-header-file
// but sometimes that might be a problem
namespace {

std::string extract_query(const fs::path &path) {
  auto comment_mark = std::string("// ");
  auto query_mark = comment_mark + std::string("Query: ");
  auto lines = utils::read_lines(path);
  // find the line with a query (the query can be split across multiple
  // lines)
  for (int i = 0; i < (int)lines.size(); ++i) {
    // find query in the line
    auto &line = lines[i];
    auto pos = line.find(query_mark);
    // if query doesn't exist pass
    if (pos == std::string::npos) continue;
    auto query = utils::trim(line.substr(pos + query_mark.size()));
    while (i + 1 < (int)lines.size() &&
           lines[i + 1].find(comment_mark) != std::string::npos) {
      query += lines[i + 1].substr(lines[i + 1].find(comment_mark) +
                                   comment_mark.length());
      ++i;
    }
    return query;
  }

  throw BasicException("Unable to find query!");
}

class CodeLineFormatException : public StacktraceException {
 public:
  using StacktraceException::StacktraceException;
};

template <typename... Args>
std::string format(const std::string &format_str, const Args &... args) {
  return fmt::format(format_str, args...);
}

template <typename... Args>
std::string code_line(const std::string &format_str, const Args &... args) {
  try {
    return "\t" + format(format_str, args...) + "\n";
  } catch (std::runtime_error &e) {
    throw CodeLineFormatException(std::string(e.what()) + " " + format_str);
  }
}

class CoutSocket {
 public:
  CoutSocket() : logger(logging::log->logger("Cout Socket")) {}

  int write(const std::string &str) {
    logger.info(str);
    return str.size();
  }

  int write(const char *data, size_t len) {
    logger.info(std::string(data, len));
    return len;
  }

  int write(const byte *data, size_t len) {
    std::stringstream ss;
    for (int i = 0; i < len; i++) {
      ss << data[i];
    }
    std::string output(ss.str());
    cout << output << endl;
    logger.info(output);
    return len;
  }

 private:
  Logger logger;
};
}
