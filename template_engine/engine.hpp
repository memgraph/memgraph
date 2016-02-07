#pragma once

#include <string>
#include <unordered_map>

#include "utils/string/replace.hpp"

namespace template_engine
{

using std::string;
using data = std::unordered_map<string, string>;

class TemplateEngine
{
public:
    string render(const string& form, const data& partials)
    {
        //  TODO more optimal implementation
        //  if more optimal implementation is to expensive
        //  use some templating engine like https://github.com/no1msd/mstch
        //  but it has to be wrapped
        string rendered = form;
        for (auto partial : partials) {
            string key = "{{" + partial.first + "}}";
            rendered = utils::replace(rendered, key, partial.second);
        }
        return rendered;
    }
};

}
