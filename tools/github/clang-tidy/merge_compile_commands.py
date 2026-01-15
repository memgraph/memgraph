#!/usr/bin/env python3
"""Merge mage compile_commands.json into main compile_commands.json.

This script merges the mage/cpp/build/compile_commands.json into the main
build/compile_commands.json, filtering out GCC-specific flags that clang-tidy
doesn't recognize.
"""

import json
import re
import sys

# GCC-specific flags that clang doesn't recognize
# These flags are safe to remove for clang-tidy analysis
GCC_ONLY_FLAG_PATTERNS = [
    r"-fvect-cost-model=\S+",  # Vectorization cost model (GCC-specific)
]


def filter_gcc_flags_from_string(cmd_str):
    """Remove GCC-specific flags from a command string."""
    if not isinstance(cmd_str, str):
        return cmd_str
    result = cmd_str
    for pattern in GCC_ONLY_FLAG_PATTERNS:
        # Remove the flag and any surrounding spaces
        result = re.sub(r"\s+" + pattern + r"(?=\s|$)", "", result)
        result = re.sub(pattern + r"(?=\s|$)", "", result)
    return result


def filter_gcc_flags_from_list(cmd_list):
    """Remove GCC-specific flags from a command list."""
    if not isinstance(cmd_list, list):
        return cmd_list
    filtered = []
    for arg in cmd_list:
        should_keep = True
        for pattern in GCC_ONLY_FLAG_PATTERNS:
            if re.match(pattern, arg):
                should_keep = False
                break
        if should_keep:
            filtered.append(arg)
    return filtered


def filter_compile_command(cmd):
    """Filter GCC-specific flags from a compile command entry."""
    if "command" in cmd:
        cmd["command"] = filter_gcc_flags_from_string(cmd["command"])
    if "arguments" in cmd:
        cmd["arguments"] = filter_gcc_flags_from_list(cmd["arguments"])
    return cmd


def main():
    """Main entry point."""
    try:
        # Read main compile_commands.json
        with open("build/compile_commands.json", "r") as f:
            main_commands = json.load(f)

        # Read mage compile_commands.json
        with open("mage/cpp/build/compile_commands.json", "r") as f:
            mage_commands = json.load(f)

        # Filter GCC-specific flags from both main and mage commands
        # (clang-tidy doesn't recognize some GCC-specific flags)
        main_commands = [filter_compile_command(cmd) for cmd in main_commands]
        mage_commands = [filter_compile_command(cmd) for cmd in mage_commands]

        # Merge the arrays
        merged_commands = main_commands + mage_commands

        # Write back to main compile_commands.json
        with open("build/compile_commands.json", "w") as f:
            json.dump(merged_commands, f, indent=2)

        print(f"Merged {len(mage_commands)} mage compile commands into main build (filtered GCC-specific flags)")
    except Exception as e:
        print(f"Warning: Failed to merge mage compile_commands.json: {e}", file=sys.stderr)
        sys.exit(0)  # Don't fail the build if merge fails


if __name__ == "__main__":
    main()
