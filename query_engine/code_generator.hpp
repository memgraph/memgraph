#pragma once

#include "utils/string/file.hpp"
#include "template_engine/engine.hpp"
#include "config/config.hpp"
#include "utils/log/logger.hpp"

using std::string;

class CodeGenerator
{
public:
    void generate(const std::string& query, const std::string& path)
    {
        string template_path = 
            config::Config::instance()[config::TEMPLATE_CPU_PATH];
        string template_file = utils::read_file(template_path.c_str());
        //  TODO instead of code should be generated code
        //  use query visitor to build the code
        string generated = template_engine.render(
            template_file,
            {
                {"class_name", "Code"},
                {"query", "\"" + query + "\""},
                {"code", "cout << \"test code\" << endl;"}
            }
        );
        utils::write_file(generated, path);
    }
private:
    template_engine::TemplateEngine template_engine;  
    Logger log;
};
