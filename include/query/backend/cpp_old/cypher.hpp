#pragma once

// DEPRICATED!
#include "config/config.hpp"
#include "query/frontend/cypher/traverser.hpp"
#include "query/language/cypher/errors.hpp"
#include "template_engine/engine.hpp"
#include "utils/string/file.hpp"
#include "utils/type_discovery.hpp"

template <typename Stream>
class CypherBackend
{
public:
    CypherBackend() : logger(logging::log->logger("CypherBackend"))
    {
        // load template file
        std::string template_path = CONFIG(config::TEMPLATE_CPU_CPP_PATH);
        template_text = utils::read_text(fs::path(template_path));
    }

    template <typename Tree>
    // TODO: query and path shoud be distinct types
    void generate_code(const Tree &tree, const std::string &query,
                       const uint64_t stripped_hash, const std::string &path)
    {
        CppTraverser cpp_traverser;
        cpp_traverser.reset();

        try {
            tree.root->accept(cpp_traverser);
        } catch (const CypherSemanticError &e) {
            throw e;
        } catch (const std::exception &e) {
            logger.error("AST traversal error: {}", std::string(e.what()));
            throw e;
        }

        // save the code
        std::string generated = template_engine::render(
            template_text.str(), {{"class_name", "CodeCPU"},
                            {"stripped_hash", std::to_string(stripped_hash)},
                            {"query", query},
                            {"stream", type_name<Stream>().to_string()},
                            {"code", cpp_traverser.code}});

        logger.trace("generated code: {}", generated);

        utils::write(utils::Text(generated), fs::path(path));
    }

protected:
    Logger logger;

private:
    utils::Text template_text;
};
