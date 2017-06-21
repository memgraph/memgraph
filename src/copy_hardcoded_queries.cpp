//
// Created by buda on 27/02/17.
//

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;

#include <glog/logging.h>

#include "query/frontend/stripped.hpp"
#include "utils/command_line/arguments.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/string.hpp"

/**
 * Reads a query from the file specified by the path argument.
 * The first line of a query should start with "// Query: ". Query can be
 * in more than one line but every line has to start with "//".
 *
 * @param path to the query file.
 * @return query as a string.
 */

std::string ExtractQuery(const fs::path &path) {
  auto comment_mark = std::string("// ");
  auto query_mark = comment_mark + std::string("Query: ");
  auto lines = utils::ReadLines(path);
  // find the line with a query (the query can be split across multiple
  // lines)
  for (int i = 0; i < static_cast<int>(lines.size()); ++i) {
    // find query in the line
    auto &line = lines[i];
    auto pos = line.find(query_mark);
    // if query doesn't exist pass
    if (pos == std::string::npos) continue;
    auto query = utils::Trim(line.substr(pos + query_mark.size()));
    while (i + 1 < static_cast<int>(lines.size()) &&
           lines[i + 1].find(comment_mark) != std::string::npos) {
      query += lines[i + 1].substr(lines[i + 1].find(comment_mark) +
                                   comment_mark.length());
      ++i;
    }
    return query;
  }

  throw utils::BasicException("Unable to find query!");
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  REGISTER_ARGS(argc, argv);

  auto src_path = fs::path(
      GET_ARG("--src", "tests/integration/hardcoded_queries").get_string());
  LOG(INFO) << "Src path is: " << src_path;
  permanent_assert(fs::exists(src_path), "src folder must exist");

  auto dst_path =
      fs::path(GET_ARG("--dst", "build/compiled/hardcode").get_string());
  LOG(INFO) << "Dst path is: " << dst_path;
  fs::create_directories(dst_path);

  auto src_files = utils::LoadFilePaths(src_path, "cpp");

  for (auto &src_file : src_files) {
    auto query = ExtractQuery(src_file);
    auto query_hash = query::StrippedQuery(query).hash();
    auto dst_file = dst_path / fs::path(std::to_string(query_hash) + ".cpp");
    fs::copy(src_file, dst_file, fs::copy_options::overwrite_existing);
    LOG(INFO) << src_file << "- {(copy) -> " << dst_file;
  }

  auto hpp_files = utils::LoadFilePaths(src_path, "hpp");
  for (auto &hpp_file : hpp_files) {
    fs::copy(hpp_file, dst_path / hpp_file.filename(),
             fs::copy_options::overwrite_existing);
  }

  return 0;
}
