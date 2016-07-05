#pragma once

#include <string>
#include <unordered_map>

namespace template_engine
{

using std::string;
using data = std::unordered_map<string, string>;

class TemplateEngine
{
public:
    string render(const string& form, const data& partials);
};

}
