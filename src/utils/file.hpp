#pragma once

#include <experimental/filesystem>
#include <fstream>
namespace fs = std::experimental::filesystem;

namespace utils {

/**
 * Loads all file paths in the specified directory. Optionally
 * the paths are filtered by extension.
 *
 * NOTE: the call isn't recursive
 *
 * @param directory a path to directory that will be scanned in order to find
 *                  all paths
 * @param extension paths will be filtered by this extension
 *
 * @return std::vector of paths founded in the directory
 */
inline auto LoadFilePaths(const fs::path &directory,
                          const std::string &extension = "") {
  // result container
  std::vector<fs::path> file_paths;

  for (auto &directory_entry : fs::recursive_directory_iterator(directory)) {
    auto path = directory_entry.path().string();

    // skip directories
    if (!fs::is_regular_file(directory_entry)) continue;

    // if extension isn't defined then put all file paths from the directory
    // to the result set
    if (!extension.empty()) {
      // skip paths that don't have appropriate extension
      auto file_extension = path.substr(path.find_last_of(".") + 1);
      if (file_extension != extension) continue;
    }

    file_paths.emplace_back(path);

    // skip paths that don't have appropriate extension
    auto file_extension = path.substr(path.find_last_of(".") + 1);
    if (file_extension != extension) continue;

    // path has the right extension and can be placed in the result
    // container
    file_paths.emplace_back(path);
  }

  return file_paths;
}

// TODO: add error checking
/**
 * Reads all lines from the file specified by path.
 *
 * @param path file path.
 * @return vector of all lines from the file.
 */
std::vector<std::string> ReadLines(const fs::path &path) {
  std::vector<std::string> lines;

  std::ifstream stream(path.c_str());
  std::string line;
  while (std::getline(stream, line)) {
    lines.emplace_back(line);
  }

  return lines;
}

/**
 * Writes test into the file specified by path.
 *
 * @param text content which will be written in the file.
 * @param path a path to the file.
 */
void Write(const std::string &text, const fs::path &path) {
  std::ofstream stream;
  stream.open(path.c_str());
  stream << text;
  stream.close();
}
}
