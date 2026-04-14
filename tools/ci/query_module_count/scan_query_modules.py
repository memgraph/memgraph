#!/usr/bin/env python3
"""Scan Memgraph/MAGE query modules and print a JSON summary."""

from __future__ import annotations

import argparse
import ast
import json
import re
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
LANGUAGES = ("python", "c", "cpp", "rust")
FILE_EXT_PATTERN = r"(?:cpp|cxx|cc|cu|hpp|hxx|hh|py|rs|c|h)"
CPP_LIKE_SUFFIXES = {".c", ".cc", ".cpp", ".cxx", ".cu", ".h", ".hh", ".hpp", ".hxx"}
_MODULE_NAME_OVERRIDES: dict[str, str] | None = None
_NEARBY_CONSTANT_CACHE: dict[tuple[str, str], str | None] = {}


def strip_comments_preserve_strings(text: str) -> str:
    """Replace comments with whitespace while preserving strings and newlines."""
    result: list[str] = []
    i = 0
    n = len(text)
    state = "code"
    quote = ""

    while i < n:
        ch = text[i]
        nxt = text[i + 1] if i + 1 < n else ""

        if state == "code":
            if ch == "/" and nxt == "/":
                result.extend("  ")
                i += 2
                state = "line_comment"
                continue
            if ch == "/" and nxt == "*":
                result.extend("  ")
                i += 2
                state = "block_comment"
                continue
            if ch in {"'", '"'}:
                quote = ch
                result.append(ch)
                i += 1
                state = "string"
                continue
            result.append(ch)
            i += 1
            continue

        if state == "line_comment":
            if ch == "\n":
                result.append("\n")
                i += 1
                state = "code"
            else:
                result.append(" ")
                i += 1
            continue

        if state == "block_comment":
            if ch == "*" and nxt == "/":
                result.extend("  ")
                i += 2
                state = "code"
            else:
                result.append("\n" if ch == "\n" else " ")
                i += 1
            continue

        if state == "string":
            result.append(ch)
            if ch == "\\" and i + 1 < n:
                result.append(text[i + 1])
                i += 2
                continue
            if ch == quote:
                state = "code"
            i += 1
            continue

    return "".join(result)


def find_matching_paren(text: str, open_idx: int) -> int | None:
    """Return the closing parenthesis index for `open_idx`, skipping strings."""
    depth = 0
    i = open_idx
    n = len(text)
    quote = ""

    while i < n:
        ch = text[i]
        if quote:
            if ch == "\\" and i + 1 < n:
                i += 2
                continue
            if ch == quote:
                quote = ""
            i += 1
            continue

        if ch in {"'", '"'}:
            quote = ch
            i += 1
            continue
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def split_top_level_args(arg_text: str) -> list[str]:
    """Split a call argument string on top-level commas only."""
    args: list[str] = []
    current: list[str] = []
    paren = bracket = brace = 0
    quote = ""
    i = 0
    n = len(arg_text)

    while i < n:
        ch = arg_text[i]

        if quote:
            current.append(ch)
            if ch == "\\" and i + 1 < n:
                current.append(arg_text[i + 1])
                i += 2
                continue
            if ch == quote:
                quote = ""
            i += 1
            continue

        if ch in {"'", '"'}:
            quote = ch
            current.append(ch)
            i += 1
            continue

        if ch == "(":
            paren += 1
        elif ch == ")":
            paren -= 1
        elif ch == "[":
            bracket += 1
        elif ch == "]":
            bracket -= 1
        elif ch == "{":
            brace += 1
        elif ch == "}":
            brace -= 1
        elif ch == "," and paren == 0 and bracket == 0 and brace == 0:
            args.append("".join(current).strip())
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        args.append(tail)
    return args


def iter_calls(text: str, pattern: str) -> list[tuple[str, list[str]]]:
    """Find matching call sites and return the callee plus parsed arguments."""
    compiled = re.compile(pattern)
    calls: list[tuple[str, list[str]]] = []
    for match in compiled.finditer(text):
        callee = match.group("callee").strip()
        open_idx = text.find("(", match.start())
        close_idx = find_matching_paren(text, open_idx)
        if close_idx is None:
            continue
        args = split_top_level_args(text[open_idx + 1 : close_idx])
        calls.append((callee, args))
    return calls


def extract_string_literal(expr: str) -> str | None:
    """Extract and decode the first double-quoted string literal in `expr`."""
    match = re.search(r'"((?:\\.|[^"\\])*)"', expr, flags=re.DOTALL)
    if not match:
        return None
    return bytes(match.group(1), "utf-8").decode("unicode_escape")


def simplify_name_expression(expr: str) -> str:
    """Strip common wrapper syntax from name expressions before lookup."""
    expr = expr.strip()
    if expr.endswith(".c_str()"):
        expr = expr[: -len(".c_str()")].strip()
    wrappers = ("std::string(", "std::string_view(", "c_str!(")
    changed = True
    while changed:
        changed = False
        for prefix in wrappers:
            if expr.startswith(prefix) and expr.endswith(")"):
                expr = expr[len(prefix) : -1].strip()
                changed = True
    return expr


def strip_hash_comments(text: str) -> str:
    """Remove `#` comments from CMake-like text one line at a time."""
    lines = []
    for line in text.splitlines():
        comment_pos = line.find("#")
        lines.append(line if comment_pos == -1 else line[:comment_pos])
    return "\n".join(lines)


def build_local_string_constants(text: str) -> dict[str, str]:
    """Collect simple local string constant definitions for later name resolution."""
    patterns = (
        (
            r"""\b(?:constexpr|const|static\s+constexpr|static\s+const)?\s*"""
            r"""[\w:\s\*&<>]*?\b([A-Za-z_]\w*)\s*=\s*"((?:\\.|[^"\\])*)"\s*;"""
        ),
        (
            r"""\b(?:constexpr|const|static\s+constexpr|static\s+const)?\s*"""
            r"""[\w:\s\*&<>]*?\b([A-Za-z_]\w*)\s*\{\s*"((?:\\.|[^"\\])*)"\s*\}\s*;"""
        ),
        r"""\b([A-Za-z_]\w*)\s*:\s*&str\s*=\s*"((?:\\.|[^"\\])*)"\s*;""",
    )
    mapping: dict[str, str] = {}
    for pattern in patterns:
        for name, value in re.findall(pattern, text):
            mapping[name] = bytes(value, "utf-8").decode("unicode_escape")
    return mapping


def tokenize_cmake_args(text: str) -> list[str]:
    """Split a CMake command body into tokens while preserving quoted strings."""
    return re.findall(r'"[^"]*"|[^\s()]+', text, flags=re.DOTALL)


def resolve_cmake_sources(expr_tokens: list[str], source_vars: dict[str, list[str]], base_dir: Path) -> list[Path]:
    """Resolve CMake source tokens and `${vars}` to concrete file paths."""
    resolved: list[Path] = []
    for token in expr_tokens:
        token = token.strip().strip('"')
        var_match = re.fullmatch(r"\$\{([^}]+)\}", token)
        if var_match:
            for source in source_vars.get(var_match.group(1), []):
                resolved.append((base_dir / source).resolve())
            continue
        if re.search(rf"\.{FILE_EXT_PATTERN}$", token):
            resolved.append((base_dir / token).resolve())
    return resolved


def build_module_name_overrides() -> dict[str, str]:
    """Build source-path to runtime-module-name overrides from CMake metadata."""
    overrides: dict[str, str] = {}

    for cmake_path in REPO_ROOT.rglob("CMakeLists.txt"):
        text = strip_hash_comments(cmake_path.read_text(encoding="utf-8"))
        source_vars: dict[str, list[str]] = {}

        for name, body in re.findall(r"set\(\s*([A-Za-z_]\w*)\s+(.*?)\)", text, flags=re.DOTALL):
            sources = re.findall(rf"([A-Za-z0-9_./-]+\.{FILE_EXT_PATTERN})", body)
            if sources:
                source_vars.setdefault(name, [])
                source_vars[name].extend(source for source in sources if source not in source_vars[name])

        for body in re.findall(r"add_custom_library\((.*?)\)", text, flags=re.DOTALL):
            tokens = tokenize_cmake_args(body)
            if len(tokens) < 2:
                continue
            target = tokens[0].strip('"')
            source = tokens[1].strip('"')
            source_path = (cmake_path.parent / source).resolve()
            overrides[relative_path(source_path)] = target

        for body in re.findall(r"add_query_module\((.*?)\)", text, flags=re.DOTALL):
            tokens = tokenize_cmake_args(body)
            if len(tokens) < 3:
                continue
            target = tokens[0].strip('"')
            for source_path in resolve_cmake_sources(tokens[2:], source_vars, cmake_path.parent):
                overrides[relative_path(source_path)] = target

        for body in re.findall(r"install\((.*?)\)", text, flags=re.DOTALL):
            if "FILES" not in body or "RENAME" not in body:
                continue
            tokens = tokenize_cmake_args(body)
            try:
                files_idx = tokens.index("FILES")
                rename_idx = tokens.index("RENAME")
            except ValueError:
                continue
            if files_idx + 1 >= len(tokens) or rename_idx + 1 >= len(tokens):
                continue
            source_token = tokens[files_idx + 1].strip('"')
            rename_token = tokens[rename_idx + 1].strip('"')
            if not source_token.endswith(".py") or not rename_token.endswith(".py"):
                continue
            source_path = (cmake_path.parent / source_token).resolve()
            overrides[relative_path(source_path)] = Path(rename_token).stem

    return overrides


def module_name_overrides() -> dict[str, str]:
    """Return the cached module-name override map, building it on first use."""
    global _MODULE_NAME_OVERRIDES
    if _MODULE_NAME_OVERRIDES is None:
        _MODULE_NAME_OVERRIDES = build_module_name_overrides()
    return _MODULE_NAME_OVERRIDES


def search_nearby_string_constant(path: Path, identifier: str) -> str | None:
    """Search nearby C/C++-like files for a uniquely defined string constant."""
    cache_key = (str(path.parent), identifier)
    if cache_key in _NEARBY_CONSTANT_CACHE:
        return _NEARBY_CONSTANT_CACHE[cache_key]

    matches: set[str] = set()
    for candidate in path.parent.rglob("*"):
        if not candidate.is_file() or candidate.suffix not in CPP_LIKE_SUFFIXES:
            continue
        text = strip_comments_preserve_strings(candidate.read_text(encoding="utf-8"))
        constants = build_local_string_constants(text)
        value = constants.get(identifier)
        if value is not None:
            matches.add(value)

    resolved = next(iter(matches)) if len(matches) == 1 else None
    _NEARBY_CONSTANT_CACHE[cache_key] = resolved
    return resolved


def resolve_registered_name(expr: str, constants: dict[str, str], path: Path | None = None) -> tuple[str | None, str]:
    """Resolve a registered name from a literal or constant-backed expression."""
    raw_expr = expr.strip()
    literal = extract_string_literal(raw_expr)
    if literal is not None:
        return literal, raw_expr

    simplified = simplify_name_expression(raw_expr)
    if simplified in constants:
        return constants[simplified], raw_expr

    suffix = simplified.split("::")[-1]
    if suffix in constants:
        return constants[suffix], raw_expr

    if path is not None:
        nearby_value = search_nearby_string_constant(path, suffix)
        if nearby_value is not None:
            return nearby_value, raw_expr

    return None, raw_expr


def relative_path(path: Path) -> str:
    """Return a repository-relative POSIX path string."""
    return path.relative_to(REPO_ROOT).as_posix()


def module_name_from_path(path: Path) -> str:
    """Infer the runtime module name from overrides, metadata, or filename."""
    rel_path = relative_path(path)
    if rel_path in module_name_overrides():
        return module_name_overrides()[rel_path]
    if path == REPO_ROOT / "src/query/procedure/module.cpp":
        return "mg"
    if path == REPO_ROOT / "src/query/stream/streams.cpp":
        return "mg"
    if path.name == "lib.rs":
        cargo = next((parent / "Cargo.toml" for parent in path.parents if (parent / "Cargo.toml").exists()), None)
        if cargo is not None:
            cargo_text = cargo.read_text(encoding="utf-8")
            lib_match = re.search(r"(?ms)^\[lib\]\s+.*?^name\s*=\s*\"([^\"]+)\"", cargo_text)
            if lib_match:
                return lib_match.group(1)
            pkg_match = re.search(r"(?ms)^\[package\]\s+.*?^name\s*=\s*\"([^\"]+)\"", cargo_text)
            if pkg_match:
                return pkg_match.group(1)
        return path.parent.parent.name
    return path.stem


def full_attr_name(node: ast.AST) -> str | None:
    """Return a dotted attribute path for simple AST name/attribute chains."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = full_attr_name(node.value)
        if parent:
            return f"{parent}.{node.attr}"
    return None


def source_segment(text: str, node: ast.AST) -> str | None:
    """Return the original source text that produced an AST node, if available."""
    return ast.get_source_segment(text, node)


def new_result_block() -> dict[str, Any]:
    """Create an empty result structure for procedures and functions by language."""
    return {
        "procedures": {language: [] for language in LANGUAGES},
        "functions": {language: [] for language in LANGUAGES},
    }


def append_item(result: dict[str, Any], category: str, language: str, item: dict[str, Any]) -> None:
    """Append one discovered item into the requested result bucket."""
    result[category][language].append(item)


def parse_python_file(path: Path, result: dict[str, Any]) -> None:
    """Parse Python decorators and batch registration calls into result entries."""
    text = path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return

    module_name = module_name_from_path(path)
    rel_path = relative_path(path)

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                decorator_name = full_attr_name(decorator)
                if decorator_name == "mgp.read_proc":
                    append_item(
                        result,
                        "procedures",
                        "python",
                        {
                            "module": module_name,
                            "name": node.name,
                            "kind": "read",
                            "path": rel_path,
                        },
                    )
                elif decorator_name == "mgp.write_proc":
                    append_item(
                        result,
                        "procedures",
                        "python",
                        {
                            "module": module_name,
                            "name": node.name,
                            "kind": "write",
                            "path": rel_path,
                        },
                    )
                elif decorator_name == "mgp.function":
                    append_item(
                        result,
                        "functions",
                        "python",
                        {
                            "module": module_name,
                            "name": node.name,
                            "kind": "function",
                            "path": rel_path,
                        },
                    )

        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
            call = node.value
            call_name = full_attr_name(call.func)
            if call_name not in {"mgp.add_batch_read_proc", "mgp.add_batch_write_proc"}:
                continue
            if not call.args:
                continue
            callback_node = call.args[0]
            callback_name = source_segment(text, callback_node) or full_attr_name(callback_node) or "<unknown>"
            append_item(
                result,
                "procedures",
                "python",
                {
                    "module": module_name,
                    "name": callback_name,
                    "kind": "batch_read" if call_name.endswith("batch_read_proc") else "batch_write",
                    "path": rel_path,
                },
            )


def parse_c_file(path: Path, result: dict[str, Any]) -> None:
    """Parse C query module registration calls from `mgp_init_module` files."""
    text = path.read_text(encoding="utf-8")
    if "mgp_init_module" not in text:
        return

    clean = strip_comments_preserve_strings(text)
    constants = build_local_string_constants(clean)
    module_name = module_name_from_path(path)
    rel_path = relative_path(path)

    patterns = {
        "read": r"(?P<callee>\bmgp_module_add_read_procedure)\s*\(",
        "write": r"(?P<callee>\bmgp_module_add_write_procedure)\s*\(",
        "batch_read": r"(?P<callee>\bmgp_module_add_batch_read_procedure)\s*\(",
        "batch_write": r"(?P<callee>\bmgp_module_add_batch_write_procedure)\s*\(",
        "function": r"(?P<callee>\bmgp_module_add_function)\s*\(",
    }

    for kind, pattern in patterns.items():
        for _callee, args in iter_calls(clean, pattern):
            if kind == "function":
                if len(args) < 3:
                    continue
                name, name_expr = resolve_registered_name(args[1], constants, path)
                callback = args[2].strip() if len(args) > 2 else None
                append_item(
                    result,
                    "functions",
                    "c",
                    {
                        "module": module_name,
                        "name": name,
                        "name_expression": name_expr if name is None else None,
                        "kind": "function",
                        "callback": callback,
                        "path": rel_path,
                    },
                )
            else:
                if len(args) < 3:
                    continue
                name, name_expr = resolve_registered_name(args[1], constants, path)
                callback = args[2].strip() if len(args) > 2 else None
                append_item(
                    result,
                    "procedures",
                    "c",
                    {
                        "module": module_name,
                        "name": name,
                        "name_expression": name_expr if name is None else None,
                        "kind": kind,
                        "callback": callback,
                        "path": rel_path,
                    },
                )


def parse_cpp_external_file(path: Path, result: dict[str, Any]) -> None:
    """Parse external C++ module registrations from high- and low-level APIs."""
    text = path.read_text(encoding="utf-8")
    if "mgp_init_module" not in text:
        return

    clean = strip_comments_preserve_strings(text)
    constants = build_local_string_constants(clean)
    module_name = module_name_from_path(path)
    rel_path = relative_path(path)

    patterns = {
        "procedure": r"(?P<callee>(?:\bmgp::)?AddProcedure)\s*\(",
        "batch": r"(?P<callee>(?:\bmgp::)?AddBatchProcedure)\s*\(",
        "function": r"(?P<callee>(?:\bmgp::)?AddFunction)\s*\(",
        "low_level_read": r"(?P<callee>\bmgp::module_add_read_procedure)\s*\(",
        "low_level_write": r"(?P<callee>\bmgp::module_add_write_procedure)\s*\(",
        "low_level_function": r"(?P<callee>\bmgp::module_add_function)\s*\(",
    }

    for _callee, args in iter_calls(clean, patterns["procedure"]):
        if len(args) < 3:
            continue
        name, name_expr = resolve_registered_name(args[1], constants, path)
        proc_type = args[2].strip()
        kind = "write" if "Write" in proc_type else "read"
        append_item(
            result,
            "procedures",
            "cpp",
            {
                "module": module_name,
                "name": name,
                "name_expression": name_expr if name is None else None,
                "kind": kind,
                "callback": args[0].strip(),
                "path": rel_path,
            },
        )

    for _callee, args in iter_calls(clean, patterns["batch"]):
        if len(args) < 5:
            continue
        name, name_expr = resolve_registered_name(args[3], constants, path)
        proc_type = args[4].strip()
        kind = "batch_write" if "Write" in proc_type else "batch_read"
        append_item(
            result,
            "procedures",
            "cpp",
            {
                "module": module_name,
                "name": name,
                "name_expression": name_expr if name is None else None,
                "kind": kind,
                "callback": args[0].strip(),
                "path": rel_path,
            },
        )

    for _callee, args in iter_calls(clean, patterns["function"]):
        if len(args) < 2:
            continue
        name, name_expr = resolve_registered_name(args[1], constants, path)
        append_item(
            result,
            "functions",
            "cpp",
            {
                "module": module_name,
                "name": name,
                "name_expression": name_expr if name is None else None,
                "kind": "function",
                "callback": args[0].strip(),
                "path": rel_path,
            },
        )

    for kind, pattern in (
        ("read", patterns["low_level_read"]),
        ("write", patterns["low_level_write"]),
    ):
        for _callee, args in iter_calls(clean, pattern):
            if len(args) < 3:
                continue
            name, name_expr = resolve_registered_name(args[1], constants, path)
            append_item(
                result,
                "procedures",
                "cpp",
                {
                    "module": module_name,
                    "name": name,
                    "name_expression": name_expr if name is None else None,
                    "kind": kind,
                    "callback": args[2].strip(),
                    "path": rel_path,
                },
            )

    for _callee, args in iter_calls(clean, patterns["low_level_function"]):
        if len(args) < 3:
            continue
        name, name_expr = resolve_registered_name(args[1], constants, path)
        append_item(
            result,
            "functions",
            "cpp",
            {
                "module": module_name,
                "name": name,
                "name_expression": name_expr if name is None else None,
                "kind": "function",
                "callback": args[2].strip(),
                "path": rel_path,
            },
        )


def parse_cpp_builtins(path: Path, result: dict[str, Any]) -> None:
    """Parse built-in `mg.*` procedures registered in `module.cpp`."""
    text = path.read_text(encoding="utf-8")
    clean = strip_comments_preserve_strings(text)
    rel_path = relative_path(path)

    for _callee, args in iter_calls(clean, r"(?P<callee>\b(?:module|builtin_module)\s*->\s*AddProcedure)\s*\("):
        if not args:
            continue
        name = extract_string_literal(args[0])
        if name is None:
            continue
        append_item(
            result,
            "procedures",
            "cpp",
            {
                "module": "mg",
                "name": name,
                "kind": "builtin",
                "path": rel_path,
            },
        )


def parse_cpp_mg_registered_procedures(path: Path, result: dict[str, Any]) -> None:
    """Parse built-in `mg.*` procedures registered via `RegisterMgProcedure`."""
    if path.suffix not in {".cpp", ".cc", ".cxx"}:
        return

    text = path.read_text(encoding="utf-8")
    if "RegisterMgProcedure" not in text:
        return

    clean = strip_comments_preserve_strings(text)
    rel_path = relative_path(path)
    found_names: set[str] = set()

    proc_name_pattern = re.compile(
        r"""proc_name\s*(?:=\s*|\{\s*)"((?:\\.|[^"\\])*)".*?RegisterMgProcedure\s*\(\s*proc_name\b""",
        flags=re.DOTALL,
    )
    for value in proc_name_pattern.findall(clean):
        found_names.add(bytes(value, "utf-8").decode("unicode_escape"))

    constants = build_local_string_constants(clean)

    for _callee, args in iter_calls(clean, r"(?P<callee>\bRegisterMgProcedure)\s*\("):
        if not args:
            continue
        name, name_expr = resolve_registered_name(args[0], constants, path)
        if name is None:
            continue
        found_names.add(name)

    for name in sorted(found_names):
        append_item(
            result,
            "procedures",
            "cpp",
            {
                "module": "mg",
                "name": name,
                "kind": "builtin",
                "path": rel_path,
            },
        )


def parse_rust_file(path: Path, result: dict[str, Any]) -> None:
    """Parse Rust query module registrations declared inside `init_module!`."""
    text = path.read_text(encoding="utf-8")
    if "init_module!" not in text:
        return

    clean = strip_comments_preserve_strings(text)
    constants = build_local_string_constants(clean)
    module_name = module_name_from_path(path)
    rel_path = relative_path(path)

    rust_proc_patterns = {
        "read": r"(?P<callee>\b\w+\s*\.\s*add_read_procedure)\s*\(",
        "write": r"(?P<callee>\b\w+\s*\.\s*add_write_procedure)\s*\(",
        "function": r"(?P<callee>\b\w+\s*\.\s*add_function)\s*\(",
    }

    for kind, pattern in rust_proc_patterns.items():
        for _callee, args in iter_calls(clean, pattern):
            if len(args) < 2:
                continue
            name, name_expr = resolve_registered_name(args[1], constants, path)
            item = {
                "module": module_name,
                "name": name,
                "name_expression": name_expr if name is None else None,
                "kind": "function" if kind == "function" else kind,
                "callback": args[0].strip(),
                "path": rel_path,
            }
            append_item(result, "functions" if kind == "function" else "procedures", "rust", item)


def sort_and_cleanup(result: dict[str, Any]) -> None:
    """Deduplicate items, drop null fields, and sort output deterministically."""
    for category in ("procedures", "functions"):
        for language in LANGUAGES:
            deduped: list[dict[str, Any]] = []
            seen: set[tuple[Any, ...]] = set()
            for item in result[category][language]:
                clean_item = {key: value for key, value in item.items() if value is not None}
                key_fields = ("module", "name", "name_expression", "kind", "path", "callback")
                key = tuple(clean_item.get(field) for field in key_fields)
                if key in seen:
                    continue
                seen.add(key)
                deduped.append(clean_item)
            deduped.sort(
                key=lambda item: (
                    item.get("module", ""),
                    item.get("name") or item.get("name_expression", ""),
                    item.get("path", ""),
                    item.get("kind", ""),
                )
            )
            result[category][language] = deduped


def add_counts(result: dict[str, Any]) -> None:
    """Add per-language and total counts to a scan result."""
    procedure_counts = {language: len(result["procedures"][language]) for language in LANGUAGES}
    function_counts = {language: len(result["functions"][language]) for language in LANGUAGES}
    result["counts"] = {
        "procedures": {**procedure_counts, "total": sum(procedure_counts.values())},
        "functions": {**function_counts, "total": sum(function_counts.values())},
        "total": sum(procedure_counts.values()) + sum(function_counts.values()),
    }


def scan_paths(paths: list[Path]) -> dict[str, Any]:
    """Scan the requested roots and aggregate all discovered registrations."""
    result = new_result_block()

    for base in paths:
        if not base.exists():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            if path.suffix == ".py":
                parse_python_file(path, result)
            elif path.suffix == ".c":
                parse_c_file(path, result)
            elif path.suffix in {".cpp", ".cc", ".cxx", ".hpp", ".hh", ".hxx"}:
                if path == REPO_ROOT / "src/query/procedure/module.cpp":
                    parse_cpp_builtins(path, result)
                parse_cpp_mg_registered_procedures(path, result)
                parse_cpp_external_file(path, result)
            elif path.suffix == ".rs":
                parse_rust_file(path, result)

    sort_and_cleanup(result)
    add_counts(result)
    return result


def build_output(target: str) -> dict[str, Any]:
    """Build the final JSON payload for the selected scan target."""
    output: dict[str, Any] = {}

    if target in {"memgraph", "all"}:
        memgraph_paths = [
            REPO_ROOT / "src",
            REPO_ROOT / "query_modules",
        ]
        output["memgraph"] = {
            "roots": [relative_path(path) for path in memgraph_paths if path.exists()],
            **scan_paths(memgraph_paths),
        }

    if target in {"mage", "all"}:
        mage_paths = [
            REPO_ROOT / "mage/python",
            REPO_ROOT / "mage/cpp",
            REPO_ROOT / "mage/rust",
        ]
        output["mage"] = {
            "roots": [relative_path(path) for path in mage_paths if path.exists()],
            **scan_paths(mage_paths),
        }

    return output


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the query module scanner."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--target",
        choices=("memgraph", "mage", "all"),
        default="all",
        help="Scan Memgraph modules, MAGE modules, or both.",
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Emit compact JSON instead of pretty-printed JSON.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the scanner CLI and print the JSON result."""
    args = parse_args()
    output = build_output(args.target)
    if args.compact:
        print(json.dumps(output, sort_keys=True))
    else:
        print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
