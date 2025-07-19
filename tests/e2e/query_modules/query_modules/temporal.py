# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

from datetime import datetime, timedelta, timezone

import mgp


@mgp.function
def zoneddatetime_to_string(val):
    return str(val)


@mgp.function
def make_zdt(year, month, day, hour, minute, second, offset_minutes):
    return datetime(year=year, month=month, day=day, hour=hour, minute=minute, second=second).astimezone(
        timezone(timedelta(minutes=offset_minutes))
    )


# @mgp.function
# def make_dt(year, month, day, hour, min, second):
#     return datetime(year=year, month=month, day=day, hour=hour, minute=min, second=second)
