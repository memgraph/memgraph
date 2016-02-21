#pragma once

#include "utils/string/file.hpp"
#include "template_engine/engine.hpp"
#include "config/config.hpp"
#include "traverser/query_traverser.hpp"

using std::string;

class CodeGenerator
{
public:

    void generate_cpp(const std::string& query, 
                      const uint64_t stripped_hash,
                      const std::string& path)
    {
        string template_path = CONFIG(config::TEMPLATE_CPU_CPP_PATH);
        string template_file = utils::read_file(template_path.c_str());
        traverser.build_tree(query);
        string code = traverser.traverse();
        string generated = template_engine.render(
            template_file,
            {
                {"class_name", "CodeCPU"},
                {"stripped_hash", std::to_string(stripped_hash)},
                {"query", query},
                {"code", code},
                {"return_type", "int"}
            }
        );
        utils::write_file(generated, path);
    }
private:
    template_engine::TemplateEngine template_engine;  
    QueryTraverser traverser;
};
