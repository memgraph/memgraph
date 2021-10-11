#!/usr/bin/python3

import argparse
import sys

BSL_HEADER = """// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt."""

MEL_HEADER = """// Copyright 2021 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="directory with source files", nargs="?")

    args = parser.parse_args()
    with open(args.file, 'r') as f:
        content = f.read()

    has_header = content.startswith(BSL_HEADER) or content.startswith(MEL_HEADER)
    if not has_header:

        def red(s):
            return f"\x1b[31m{s}\x1b[0m"

        sys.stdout.writelines(red("The file is missing a header. Please add the BSL or MEL license header!\n"))
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()
