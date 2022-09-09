## Generate Antlr openCypher parser

set(opencypher_frontend ${CMAKE_CURRENT_SOURCE_DIR}/opencypher)
set(opencypher_generated ${opencypher_frontend}/generated)
set(opencypher_lexer_grammar ${opencypher_frontend}/grammar/MemgraphCypherLexer.g4)
set(opencypher_parser_grammar ${opencypher_frontend}/grammar/MemgraphCypher.g4)

set(antlr_opencypher_generated_src
    ${opencypher_generated}/MemgraphCypherLexer.cpp
    ${opencypher_generated}/MemgraphCypher.cpp
    ${opencypher_generated}/MemgraphCypherBaseVisitor.cpp
    ${opencypher_generated}/MemgraphCypherVisitor.cpp
)
set(antlr_opencypher_generated_include
    ${opencypher_generated}/MemgraphCypherLexer.h
    ${opencypher_generated}/MemgraphCypher.h
    ${opencypher_generated}/MemgraphCypherBaseVisitor.h
    ${opencypher_generated}/MemgraphCypherVisitor.h
)

add_custom_command(
    OUTPUT ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${opencypher_generated}
    COMMAND
        java -jar ${CMAKE_SOURCE_DIR}/libs/antlr-4.10.1-complete.jar
            -Dlanguage=Cpp -visitor -package antlropencypher
            -o ${opencypher_generated}
            ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    DEPENDS
        ${opencypher_lexer_grammar} ${opencypher_parser_grammar}
        ${opencypher_frontend}/grammar/CypherLexer.g4
        ${opencypher_frontend}/grammar/Cypher.g4)

add_custom_target(generated_opencypher_parser
    DEPENDS ${antlr_opencypher_generated_src} ${antlr_opencypher_generated_include})

add_library(mg-parser STATIC ${antlr_opencypher_generated_src})
add_dependencies(mg-parser generated_opencypher_parser)
target_include_directories(mg-parser PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(mg-parser antlr4)
