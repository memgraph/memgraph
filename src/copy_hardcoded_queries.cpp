//
// Created by buda on 27/02/17.
//

#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#include "logging/streams/stdout.hpp"
#include "query/preprocessor.hpp"
#include "query/util.hpp"
#include "utils/command_line/arguments.hpp"

int main(int argc, char **argv) {
  logging::init_sync();
  logging::log->pipe(std::make_unique<Stdout>());

  auto logger = logging::log->logger("CopyHardcodedQueries");
  logger.info("{}", logging::log->type());

  REGISTER_ARGS(argc, argv);

  auto src_path = fs::path(
      GET_ARG("--src", "tests/integration/hardcoded_queries").get_string());
  logger.info("Src path is: {}", src_path);
  permanent_assert(fs::exists(src_path), "src folder must exist");

  auto dst_path =
      fs::path(GET_ARG("--dst", "build/compiled/hardcode").get_string());
  logger.info("Dst path is: {}", dst_path);
  fs::create_directories(dst_path);

  auto src_files = utils::LoadFilePaths(src_path, "cpp");

  QueryPreprocessor preprocessor;
  for (auto &src_file : src_files) {
    auto query = extract_query(src_file);
    auto query_hash = preprocessor.preprocess(query).hash;
    auto dst_file = dst_path / fs::path(std::to_string(query_hash) + ".cpp");
    fs::copy(src_file, dst_file, fs::copy_options::overwrite_existing);
    logger.info("{} - (copy) -> {}", src_file, dst_file);
  }

  auto hpp_files = utils::LoadFilePaths(src_path, "hpp");
  for (auto &hpp_file : hpp_files) {
    fs::copy(hpp_file, dst_path / hpp_file.filename(),
             fs::copy_options::overwrite_existing);
  }

  return 0;
}