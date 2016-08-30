#pragma once

#include "config/config.hpp"
#include "cypher/ast/ast.hpp"
#include "cypher/compiler.hpp"
#include "query_engine/exceptions/errors.hpp"
#include "template_engine/engine.hpp"
#include "traverser/cpp_traverser.hpp"
#include "utils/string/file.hpp"
#include "logging/default.hpp"
#include "utils/type_discovery.hpp"

using std::string;

template <typename Stream>
class CodeGenerator
{
public:
    CodeGenerator() : logger(logging::log->logger("CodeGenerator")) {}

    void generate_cpp(const std::string &query, const uint64_t stripped_hash,
                      const std::string &path)
    {
        // TODO: optimize; one time initialization -> be careful that object
        // has a state
        // TODO: multithread test
        CppTraverser cpp_traverser;

        // get paths
        string template_path = CONFIG(config::TEMPLATE_CPU_CPP_PATH);
        string template_file = utils::read_file(template_path.c_str());

        // syntax tree generation
        try {
            tree = compiler.syntax_tree(query);
        } catch (const std::runtime_error &e) {
            logger.error("Syntax error: {}", query);
            throw QueryEngineException(std::string(e.what()));
        }

        cpp_traverser.reset();

        // code generation
        try {
            tree.root->accept(cpp_traverser);
        } catch (const SemanticError &e) {
            throw e;
        } catch (const std::exception &e) {
            logger.error("AST traversal error: {}", std::string(e.what()));
            throw QueryEngineException("Unknown code generation error");
        }

        // save the code
        string generated = template_engine.render(
            template_file, {{"class_name", "CodeCPU"},
                            {"stripped_hash", std::to_string(stripped_hash)},
                            {"query", query},
                            {"stream", type_name<Stream>().to_string()},
                            {"code", cpp_traverser.code}});

        logger.debug("generated code: {}", generated);

        utils::write_file(generated, path);
    }

protected:
    Logger logger;

private:
    template_engine::TemplateEngine template_engine;
    ast::Ast tree;
    cypher::Compiler compiler;
};
