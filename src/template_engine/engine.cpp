#include "template_engine/engine.hpp"

#include "utils/string.hpp"

namespace template_engine {

/**
 * Replaces all placeholders in the form string marked as {{ key }} with values
 * defined in the partials dictionary.
 *
 * @param form template string.
 * @param partials values to inject into the template string.
 * @return string rendered based on template string and values.
 */
std::string Render(const std::string &form, const data &partials) {
  //  TODO more optimal implementation
  //  another option is something like https://github.com/no1msd/mstch
  //  but it has to be wrapped
  string rendered = form;
  for (const auto &partial : partials) {
    string key = "{{" + partial.first + "}}";
    rendered = utils::Replace(rendered, key, partial.second);
  }
  return rendered;
}
}
