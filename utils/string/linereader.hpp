#pragma once

#include <functional>
#include <fstream>
#include <string>

namespace utils
{

void linereader(std::istream& stream,
                std::function<void(const std::string&)> cb)
{
    std::string line;

    while(std::getline(stream, line))
        cb(line);
}

void linereader(const std::string& filename,
                std::function<void(const std::string&)> cb)
{
    std::fstream fs(filename.c_str());

    // should this throw an error? figure out how to handle this TODO

    if(fs.is_open() == false)
        throw std::runtime_error("[ERROR] can't open file " + filename);

    linereader(fs, cb);
}

}
