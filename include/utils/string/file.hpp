#pragma once

#include <cerrno>
// TODO: remove experimental from here once that becomes possible
#include <experimental/filesystem>
#include <fstream>
#include <ostream>
#include <stdexcept>
#include <streambuf>
#include <string>

#include <fmt/format.h>

// TODO: remove experimental from here once it becomes possible
namespace fs = std::experimental::filesystem;

namespace utils
{

/*
 * Type safe text object.
 */
class Text
{
public:
    Text() = default;
    explicit Text(const std::string &text) : text_(text) {}

    // text could be huge and copy operationt would be too expensive
    Text(const Text& other) = delete;
    Text& operator=(const Text& other) = delete;

    // the object is movable
    Text (Text&& other) = default;
    Text& operator=(Text&& other)
    {
        text_ = std::move(other.text_);
        return *this;
    }

    const std::string &str() const { return text_; }

private:
    std::string text_;
};

/*
 * Reads the whole text from a file at the path.
 */
Text read_text(const fs::path &path);

/*
 * Reads all the lines from a file at the path.
 */
std::vector<std::string> read_lines(const fs::path &path);

// TODO: lazy implementation of read_lines functionality (line by line)

// TODO: read word by word + lazy implementation

/*
 * Write text in a file at the path.
 */
void write(const Text &text, const fs::path &path);

}
