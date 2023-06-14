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

import threading

import neo4j


def check_md(tx_md):
    n = 0
    for record in tx_md:
        md = record[3]
        if md["ver"] == "session" and md["str"] == "aha" and md["num"] == 123:
            n = n + 1
        elif md["ver"] == "transaction" and md["str"] == "oho" and md["num"] == 456:
            n = n + 1
    return n


def session_run(driver):
    print("Checking implicit transaction metadata...")
    with driver.session() as session:
        query = neo4j.Query("SHOW TRANSACTIONS", timeout=2, metadata={"ver": "session", "str": "aha", "num": 123})
        result = session.run(query).values()
        assert check_md(result) == 1, "metadata info error!"


def show_tx(driver, tx_md):
    with driver.session() as session:
        query = neo4j.Query("SHOW TRANSACTIONS", timeout=2, metadata={"ver": "session", "str": "aha", "num": 123})
        for t in session.run(query).values():
            tx_md.append(t)


def transaction_run(driver):
    print("Checking explicit transaction metadata...")
    with driver.session() as session:
        tx = session.begin_transaction(timeout=2, metadata={"ver": "transaction", "str": "oho", "num": 456})
        tx.run("MATCH (n) RETURN n LIMIT 1").consume()
        tx_md = []
        th = threading.Thread(target=show_tx, args=(driver, tx_md))
        th.start()
        if th.is_alive():
            th.join()
        tx.commit()
        assert check_md(tx_md) == 2, "metadata info error!"


if __name__ == "__main__":
    driver = neo4j.GraphDatabase.driver("bolt://localhost:7687", auth=("user", "pass"), encrypted=False)
    session_run(driver)
    transaction_run(driver)
    driver.close()
    print("All ok!")
