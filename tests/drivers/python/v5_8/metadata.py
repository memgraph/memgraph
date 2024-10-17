#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import re
import threading

from memgraph_utils import *
from neo4j import GraphDatabase, Query
from neo4j.exceptions import ClientError, TransientError


def check_md(tx_md):
    n = 0
    for record in tx_md:
        md = record[3]
        if md["ver"] == "session" and md["str"] == "aha" and md["num"] == 123:
            n = n + 1
        elif md["ver"] == "transaction" and md["str"] == "oho" and md["num"] == 456:
            n = n + 1
    return n


def check_md_logged(metadata):
    return (metadata["ver"] == '"session"' and metadata["str"] == '"aha"' and metadata["num"] == "123") or (
        metadata["ver"] == '"transaction"' and metadata["str"] == '"oho"' and metadata["num"] == "456"
    )


def session_run(session):
    print("Checking implicit transaction metadata...")
    query = Query("SHOW TRANSACTIONS", timeout=2, metadata={"ver": "session", "str": "aha", "num": 123})
    result = session.run(query).values()
    assert check_md(result) == 1, "metadata info error!"


def show_tx(driver, tx_md):
    with driver.session() as session:
        query = Query("SHOW TRANSACTIONS", timeout=2, metadata={"ver": "session", "str": "aha", "num": 123})
        for t in session.run(query).values():
            tx_md.append(t)


def transaction_run(session, driver):
    print("Checking explicit transaction metadata...")
    tx = session.begin_transaction(timeout=2, metadata={"ver": "transaction", "str": "oho", "num": 456})
    tx.run("MATCH (n) RETURN n LIMIT 1").consume()
    tx_md = []
    th = threading.Thread(target=show_tx, args=(driver, tx_md))
    th.start()
    if th.is_alive():
        th.join()
    tx.commit()
    assert check_md(tx_md) == 2, "metadata info error!"


def parse_metadata_to_dict(metadata_str):
    # Split the info string by commas to get key:val pairs
    key_val_pairs = metadata_str.split(",")
    # Create a dictionary by splitting each pair by ":"
    info_dict = {}
    for pair in key_val_pairs:
        key, value = pair.split(":")
        info_dict[key.strip()] = value.strip()  # Strip any whitespace

    return info_dict


def trace_on(session):
    old_log_level = get_log_level(session)
    set_log_level(session, "TRACE")
    return old_log_level


def trace_off(session, old_log_level):
    set_log_level(session, old_log_level)


def check_logs(session):
    log_path = get_log_file_path(session)
    assert len(log_path) != 0, "No log file found! Test requires logging to file."
    metadata_pattern = re.compile(r"\[Run.*\{(.*)\}\s*$")
    md_lines = 0
    with open(log_path) as log_file:
        for line in log_file:
            match = metadata_pattern.search(line)
            if match:
                metadata = parse_metadata_to_dict(match.group(1))
                md_lines += int(check_md_logged(metadata))
    return md_lines


if __name__ == "__main__":
    with GraphDatabase.driver("bolt://localhost:7687", auth=None, encrypted=False) as driver:
        with driver.session() as session:
            old_md_lines = check_logs(session)
            old_log_level = trace_on(session)
            session_run(session)
            transaction_run(session, driver)
            md_lines = check_logs(session)
            assert md_lines - old_md_lines == 3, f"Found {md_lines - old_md_lines} metadata lines instead of 3"
            trace_off(session, old_log_level)
    print("All ok!")
