# Copyright 2022 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import mgp


@mgp.function
def return_function_argument(ctx: mgp.FuncCtx, argument: mgp.Any):
    return argument


@mgp.function
def add_two_numbers(ctx: mgp.FuncCtx, first: mgp.Number, second: mgp.Number):
    return first + second


@mgp.function
def return_null(ctx: mgp.FuncCtx):
    return None
