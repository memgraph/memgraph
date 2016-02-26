#pragma once

#include "cypher/ast/ast.hpp"
#include "cypher/compiler.hpp"
#include "utils/string/file.hpp"
#include "template_engine/engine.hpp"
#include "config/config.hpp"
#include "traverser/code_traverser.hpp"

using std::string;

class CodeGenerator
{
public:
    void generate_cpp(const std::string& query, 
                      const uint64_t stripped_hash,
                      const std::string& path)
    {
        // get paths
        string template_path = CONFIG(config::TEMPLATE_CPU_CPP_PATH);
        string template_file = utils::read_file(template_path.c_str());

        // traversing
        auto tree = compiler.syntax_tree(query);
        code_traverser.reset();
        tree.root->accept(code_traverser);

        // save the code
        string generated = template_engine.render(
            template_file,
            {
                {"class_name", "CodeCPU"},
                {"stripped_hash", std::to_string(stripped_hash)},
                {"query", query},
                {"code", code_traverser.code}
            }
        );
        utils::write_file(generated, path);
    }

private:
    template_engine::TemplateEngine template_engine;  
    ast::Ast tree;
    cypher::Compiler compiler;
    CodeTraverser code_traverser;
};
