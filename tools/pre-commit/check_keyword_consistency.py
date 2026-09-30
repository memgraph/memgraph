#!/usr/bin/env python3
"""
Script to check consistency of keywords across grammar files.

Ensures that all lexer tokens defined in MemgraphCypherLexer.g4 are properly
included in both MemgraphCypher.g4 as a memgraphCypherKeyword rule, and
in stripped_lexer_constants.hpp

"""

import re
import sys
from pathlib import Path


def extract_lexer_tokens(lexer_file):
    tokens = set()
    with open(lexer_file, "r") as f:
        content = f.read()

    pattern = r"^([A-Z_]+)\s*:\s*(?:[A-Z]\s*)+;"
    for match in re.finditer(pattern, content, re.MULTILINE):
        token = match.group(1)
        if token not in [
            "COLON",
            "SEMICOLON",
            "COMMA",
            "DOT",
            "PLUS",
            "MINUS",
            "ASTERISK",
            "SLASH",
            "PERCENT",
            "CARET",
            "EQ",
            "NE",
            "LT",
            "LE",
            "GT",
            "GE",
            "LBRACE",
            "RBRACE",
            "LPAREN",
            "RPAREN",
            "LBRACKET",
            "RBRACKET",
            "UNDERSCORE",
            "PIPE",
            "AMPERSAND",
            "EXCLAMATION",
            "QUESTION",
            "UnescapedSymbolicName",
            "EscapedSymbolicName",
            "StringLiteral",
            "DecimalLiteral",
            "OctalLiteral",
            "HexadecimalLiteral",
            "BooleanLiteral",
            "DoubleLiteral",
            "Whitespace",
            "Comment",
        ]:
            tokens.add(token)

    return tokens


def extract_grammar_keywords(grammar_file):
    keywords = set()
    with open(grammar_file, "r") as f:
        content = f.read()

    pattern = r"memgraphCypherKeyword\s*:(.*?);"
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        raise RuntimeError("Could not find memgraphCypherKeyword rule")

    rule_content = match.group(1)

    token_pattern = r"\|\s*([A-Z_]+)"
    for match in re.finditer(token_pattern, rule_content):
        keywords.add(match.group(1))

    return keywords


def extract_constants_keywords(constants_file):
    keywords = set()
    with open(constants_file, "r") as f:
        content = f.read()

    pattern = r"const trie::Trie kKeywords = \{(.*?)\};"
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        raise RuntimeError("Could not find kKeywords trie")

    trie_content = match.group(1)

    keyword_pattern = r'"([a-z_]+)"'
    for match in re.finditer(keyword_pattern, trie_content):
        keywords.add(match.group(1).upper())

    return keywords


def main():
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent.parent

    lexer_file = repo_root / "src/query/frontend/opencypher/grammar/MemgraphCypherLexer.g4"
    grammar_file = repo_root / "src/query/frontend/opencypher/grammar/MemgraphCypher.g4"
    constants_file = repo_root / "src/query/frontend/stripped_lexer_constants.hpp"

    for file in [lexer_file, grammar_file, constants_file]:
        if not file.exists():
            print(f"ERROR: File not found: {file}")
            return 1

    try:
        lexer_tokens = extract_lexer_tokens(lexer_file)
        grammar_keywords = extract_grammar_keywords(grammar_file)
        constants_keywords = extract_constants_keywords(constants_file)

        missing_in_grammar = lexer_tokens - grammar_keywords
        missing_in_constants = lexer_tokens - constants_keywords

        success = True

        if missing_in_grammar:
            print(f"\nERROR: {len(missing_in_grammar)} tokens missing from memgraphCypherKeyword rule:")
            for token in sorted(missing_in_grammar):
                print(f"  - {token}")
            success = False

        if missing_in_constants:
            print(f"\nERROR: {len(missing_in_constants)} tokens missing from kKeywords trie:")
            for token in sorted(missing_in_constants):
                print(f"  - {token.lower()}")
            success = False

        if success:
            return 0
        else:
            return 1

    except Exception as e:
        print(f"ERROR: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
