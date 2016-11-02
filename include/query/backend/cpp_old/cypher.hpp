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
#ifdef BARRIER
        std::string template_path =
            CONFIG(config::BARRIER_TEMPLATE_CPU_CPP_PATH);
#else
        std::string template_path = CONFIG(config::TEMPLATE_CPU_CPP_PATH);
#endif
        template_file = utils::read_file(template_path.c_str());
    }

    template <typename Tree>
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
            template_file, {{"class_name", "CodeCPU"},
                            {"stripped_hash", std::to_string(stripped_hash)},
                            {"query", query},
#ifdef BARRIER
                            {"stream", "RecordStream<::io::Socket>"},
#else
                            {"stream", type_name<Stream>().to_string()},
#endif
                            {"code", cpp_traverser.code}});

        logger.trace("generated code: {}", generated);

        utils::write_file(generated, path);
    }

protected:
    Logger logger;

private:
    std::string template_file;
};
