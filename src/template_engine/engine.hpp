#pragma once

#include <string>
#include <unordered_map>

namespace template_engine {

using std::string;
using data = std::unordered_map<std::string, std::string>;

std::string Render(const string& form, const data& partials);
}
