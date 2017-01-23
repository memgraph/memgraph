#include "utils/string/file.hpp"

#include <iterator>

namespace utils {

namespace fs = std::experimental::filesystem;

Text read_text(const fs::path &path) {
  std::ifstream in(path, std::ios::in | std::ios::binary);

  if (in)
    return Text(std::string(std::istreambuf_iterator<char>(in),
                            std::istreambuf_iterator<char>()));

  auto error_message = fmt::format("{0}{1}", "Fail to read: ", path.c_str());
  throw std::runtime_error(error_message);
}

std::vector<std::string> read_lines(const fs::path &path) {
  std::vector<std::string> lines;

  std::ifstream stream(path.c_str());
  std::string line;
  while (std::getline(stream, line)) {
    lines.emplace_back(line);
  }

  return lines;
}

void write(const Text &text, const fs::path &path) {
  std::ofstream stream;
  stream.open(path.c_str());
  stream << text.str();
  stream.close();
}
}
