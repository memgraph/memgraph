#!/usr/bin/python3

import argparse
import re
import sys
from datetime import datetime
from pathlib import Path
from string import Template

BSL_HEADER = Template(
    """// Copyright $year Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt."""
)

MEL_HEADER = Template(
    """// Copyright $year Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
"""
)


def is_header_correct(content, header):
    return content.startswith(header)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def replace_header(tmp_path, real_path, year):
    with open(tmp_path) as f:
        lines = f.readlines()
    lines[0] = f"// Copyright {year} Memgraph Ltd.\n"
    with open(real_path, "w") as f:
        f.writelines(lines)


def red(s):
    return f"\x1b[31m{s}\x1b[0m"


def yellow(s):
    return f"\x1b[33m{s}\x1b[0m"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File to check.", nargs="?")
    parser.add_argument("real_file", help="Real path to the file.", nargs="?")
    parser.add_argument(
        "--amend-year",
        type=str2bool,
        default=False,
        const=True,
        nargs="?",
        help="Will modify the license if only year is not correct.",
    )

    args = parser.parse_args()
    with open(args.file, "r") as f:
        content = f.read()

    year = datetime.today().year
    bls_header_complete = BSL_HEADER.substitute({"year": year})
    mel_header_complete = MEL_HEADER.substitute({"year": year})

    has_header = is_header_correct(content, bls_header_complete) or is_header_correct(content, mel_header_complete)
    if not has_header:

        if args.amend_year:
            replaced_content = re.sub(r"Copyright [0-9]{4}", f"Copyright {year}", content, 1)
            is_header_corrected = is_header_correct(replaced_content, bls_header_complete) or is_header_correct(
                replaced_content, mel_header_complete
            )
            if is_header_corrected:
                replace_header(args.file, args.real_file, year)
                sys.stdout.writelines(yellow(f"Changing year in header for {args.real_file}!\n"))
                sys.exit(1)

        sys.stdout.writelines(
            red("The file is missing a correct header. Please add/check the BSL or MEL license header!\n")
        )
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
