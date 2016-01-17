/*
** This file contains memgraph's grammar for Cypher.  Process this file
** using the lemon parser generator to generate C code that runs
** the parser. Lemon will also generate a header file containing
** numeric codes for all of the tokens.
*/

%token_prefix TK_

%token_type {Token*}

%extra_argument {ast::Ast* ast}

%syntax_error
{
    int n = sizeof(yyTokenName) / sizeof(yyTokenName[0]);
    for (int i = 0; i < n; ++i) {
            int a = yy_find_shift_action(yypParser, (YYCODETYPE)i);
            if (a < YYNSTATE + YYNRULE) {
                    printf("possible token: %s\n", yyTokenName[i]);
            }
    }
    // throw SyntaxError(TOKEN->value);
}

%stack_overflow
{
    throw ParserError("Parser stack overflow");
}

%name cypher_parser

%include
{
    #include <iostream>
    #include <cassert>
    #include <cstdlib>

    #include "token.hpp"
    #include "errors.hpp"
    #include "ast/ast.hpp"
    #include "ast/tree.hpp"

    #define DEBUG(X) std::cout << "PARSER: " << X << std::endl
}

// define operator precedence
%left OR.
%left AND.
%right NOT.
%left IN IS_NULL IS_NOT_NULL NE EQ.
%left GT LE LT GE.
%left PLUS MINUS.
%left STAR SLASH REM.

start ::= read_query(RQ). {
   ast->root = ast->create<ast::Start>(RQ, nullptr);
}

start ::= write_query(WQ). {
   ast->root = ast->create<ast::Start>(nullptr, WQ);
}

// write query structure
%type write_query {ast::WriteQuery*}

write_query(WQ) ::= create_clause(C) return_clause(R). {
    WQ = ast->create<ast::WriteQuery>(C, R);
}

write_query(WQ) ::= create_clause(C). {
    WQ = ast->create<ast::WriteQuery>(C, nullptr);
}

%type create_clause {ast::Create*}

create_clause(C) ::= CREATE pattern(P). {
   C = ast->create<ast::Create>(P);
}

// read query structure
%type read_query {ast::ReadQuery*}

read_query(RQ) ::= match_clause(M) return_clause(R). {
    RQ = ast->create<ast::ReadQuery>(M, R);
}

%type match_clause {ast::Match*}

match_clause(M) ::= MATCH pattern(P) where_clause(W). {
    M = ast->create<ast::Match>(P, W);
}

%type pattern {ast::Pattern*}

// pattern specification
pattern(P) ::= node(N) rel(R) pattern(NEXT). {
    P = ast->create<ast::Pattern>(N, R, NEXT);
}

pattern(P) ::= node(N). {
    P = ast->create<ast::Pattern>(N, nullptr, nullptr);
}

%type rel {ast::Relationship*}

rel(R) ::= MINUS rel_spec(S) MINUS. { // bidirectional
    R = ast->create<ast::Relationship>(S, ast::Relationship::Both);
}

rel(R) ::= LT MINUS rel_spec(S) MINUS. { // left
    R = ast->create<ast::Relationship>(S, ast::Relationship::Left);
}

rel(R) ::= MINUS rel_spec(S) MINUS GT. { // right
    R = ast->create<ast::Relationship>(S, ast::Relationship::Right);
}

%type rel_spec {ast::RelationshipSpecs*}

rel_spec(R) ::= LSP rel_idn(I) rel_type(T) properties(P) RSP. {
   R = ast->create<ast::RelationshipSpecs>(I, T, P);
}

rel_spec(R) ::= . {
    R = nullptr;
}

%type rel_idn {ast::Identifier*}

rel_idn(R) ::= idn(I). {
    R = I;
}

rel_idn(R) ::= . {
    R = nullptr;
}

%type rel_type {ast::RelationshipList*}

rel_type(L) ::= COLON rel_list(R). {
    L = R;
}

rel_type(L) ::= . {
    L = nullptr;
}

%type rel_list {ast::RelationshipList*}

rel_list(L) ::= idn(I) PIPE rel_list(R). {
    L = ast->create<ast::RelationshipList>(I, R);
}

rel_list(L) ::= idn(I). {
    L = ast->create<ast::RelationshipList>(I, nullptr);
}

%type node {ast::Node*}

// node specification
node(N) ::= LP node_idn(I) label_idn(L) properties(P) RP. {
    N = ast->create<ast::Node>(I, L, P);
}

node(N) ::= idn(I). {
    N = ast->create<ast::Node>(I, nullptr, nullptr);
}

%type node_idn {ast::Identifier*}

// a node identifier can be ommitted
node_idn(N) ::= idn(I). {
    N = I;
}

node_idn(N) ::= . {
    N = nullptr;
}

%type label_idn {ast::LabelList*}

// a label can be ommited or there can be more of them
label_idn(L) ::= COLON idn(I) label_idn(N). {
    L = ast->create<ast::LabelList>(I, N);
}

label_idn(L) ::= . {
    L = nullptr;
}

%type where_clause {ast::Where*}

// where clause
where_clause(W) ::= WHERE expr(E). {
    W = ast->create<ast::Where>(E);
}

where_clause(W) ::= . {
    W = nullptr;
}

%type return_clause {ast::Return*}

return_clause(R) ::= RETURN return_list(L). {
    R = ast->create<ast::Return>(L);
}

return_clause(R) ::= RETURN distinct(D). {
    R = ast->create<ast::Return>(D);
}

%type return_list {ast::ReturnList*}

return_list(R) ::= return_list(N) COMMA idn(I). {
    R = ast->create<ast::ReturnList>(I, N);
}

return_list(R) ::= idn(I). {
    R = ast->create<ast::ReturnList>(I, nullptr);
}

%type distinct {ast::Distinct*}

distinct(R) ::= DISTINCT idn(I). {
    R = ast->create<ast::Distinct>(I);
}

%type properties {ast::PropertyList*}

// '{' <property_list> '}'
properties(P) ::= LCP property_list(L) RCP. {
    P = L;
}

properties(P) ::= . {
    P = nullptr;
}

%type property_list {ast::PropertyList*}

// <property> [[',' <property>]]*
property_list(L) ::= property(P) COMMA property_list(N). {
    L = ast->create<ast::PropertyList>(P, N);
}

property_list(L) ::= property(P). {
    L = ast->create<ast::PropertyList>(P, nullptr);
}

%type property {ast::Property*}

// IDENTIFIER ':' <expression>
property(P) ::= idn(I) COLON expr(E). {
    P = ast->create<ast::Property>(I, E);
}

%type expr {ast::Expr*}

expr(E) ::= expr(L) AND expr(R). {
    E = ast->create<ast::And>(L, R);
}

expr(E) ::= expr(L) OR expr(R). {
    E = ast->create<ast::Or>(L, R);
}

expr(E) ::= expr(L) LT expr(R). {
    E = ast->create<ast::Lt>(L, R);
}

expr(E) ::= expr(L) GT expr(R). {
    E = ast->create<ast::Gt>(L, R);
}

expr(E) ::= expr(L) GE expr(R). {
    E = ast->create<ast::Ge>(L, R);
}

expr(E) ::= expr(L) LE expr(R). {
    E = ast->create<ast::Le>(L, R);
}

expr(E) ::= expr(L) EQ expr(R). {
    E = ast->create<ast::Eq>(L, R);
}

expr(E) ::= expr(L) NE expr(R). {
    E = ast->create<ast::Ne>(L, R);
}

expr(E) ::= expr(L) PLUS expr(R). {
    E = ast->create<ast::Plus>(L, R);
}

expr(E) ::= expr(L) MINUS expr(R). {
    E = ast->create<ast::Minus>(L, R);
}

expr(E) ::= expr(L) STAR expr(R). {
    E = ast->create<ast::Star>(L, R);
}

expr(E) ::= expr(L) SLASH expr(R). {
    E = ast->create<ast::Slash>(L, R);
}

expr(E) ::= expr(L) REM expr(R). {
    E = ast->create<ast::Rem>(L, R);
}

expr(E) ::= idn(I) DOT idn(P). {
    E = ast->create<ast::Accessor>(I, P);
}

%type idn {ast::Identifier*}

idn(I) ::= IDN(X). {
    I = ast->create<ast::Identifier>(X->value);
}

expr(E) ::= INT(V). {
    auto value = std::stoi(V->value);
    E = ast->create<ast::Integer>(value);
}

expr(E) ::= FLOAT(V). {
    auto value = std::stod(V->value);
    E = ast->create<ast::Float>(value);
}

expr(E) ::= STR(V). {
    auto value = V->value.substr(1, V->value.size() - 2);
    E = ast->create<ast::String>(value);
}

expr(E) ::= BOOL(V). {
    auto value = V->value[0] == 't' || V->value[0] == 'T' ? true : false;
    E = ast->create<ast::Boolean>(value);
}

