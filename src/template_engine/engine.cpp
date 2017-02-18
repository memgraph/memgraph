#include "template_engine/engine.hpp"

#include "utils/string/replace.hpp"

namespace template_engine {

string render(const string& form, const data& partials) {
  //  TODO more optimal implementation
  //  another option is something like https://github.com/no1msd/mstch
  //  but it has to be wrapped
  string rendered = form;
  for (auto partial : partials) {
    string key = "{{" + partial.first + "}}";
    rendered = utils::replace(rendered, key, partial.second);
  }
  return rendered;
}
}
