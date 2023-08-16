#!/usr/bin/env python3

import argparse
import os
import subprocess
import sys

CLANG_TIDY_DIFF = "./tools/github/clang-tidy/clang-tidy-diff.py"


def check_clang_tidy():
    try:
        subprocess.run(["clang-tidy", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except FileNotFoundError:
        print("clang-tidy is not installed. Please install clang-tidy and try again.", file=sys.stderr)
        sys.exit(1)
    except subprocess.CalledProcessError:
        print("Error while checking clang-tidy version.", file=sys.stderr)
        sys.exit(1)

    if not os.path.exists(CLANG_TIDY_DIFF):
        print(f"Error can't find '{CLANG_TIDY_DIFF}'.", file=sys.stderr)
        sys.exit(1)


def run_clang_tidy_on_files(compile_commands_path):
    process1 = subprocess.Popen("git diff -U0 HEAD".split(), stdout=subprocess.PIPE)
    process2 = subprocess.Popen(
        f"{CLANG_TIDY_DIFF} -p1 -j 8 -path {compile_commands_path}".split(),
        stdin=process1.stdout,
        stdout=subprocess.PIPE,
    )
    process1.stdout.close()  # Close the output pipe of the first process
    output, err_output = process2.communicate()  # Get the output from the second process
    if process2.returncode != 0:
        print("Error occurred in clang-tidy-diff command:")
        if output:
            print(output.decode().strip())
        if err_output:
            print(err_output.decode().strip(), file=sys.stderr)
        sys.exit(1)


def main():
    check_clang_tidy()

    parser = argparse.ArgumentParser(description="Run clang-tidy on specified files.")
    # parser.add_argument("file_paths", nargs="+", type=str, help="Paths to files to be checked.")
    parser.add_argument("--compile_commands_path", type=str, required=True, help="Path to compile_commands.json.")
    args = parser.parse_args()

    run_clang_tidy_on_files(args.compile_commands_path)


if __name__ == "__main__":
    main()
