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

import random

def init_data(card_count, pos_count):
    print("UNWIND range(0, {} - 1) AS id "
        "CREATE (:Card {{id: id, compromised: false}});".format(
            card_count))
    print("UNWIND range(0, {} - 1) AS id "
        "CREATE (:Pos {{id: id, compromised: false}});".format(
            pos_count))


def compromise_pos_device(pos_id):
    print("MATCH (p:Pos {{id: {}}}) SET p.compromised = true;".format(pos_id))


def compromise_pos_devices(pos_count, fraud_count):
    for pos_id in random.sample(range(pos_count), fraud_count):
        compromise_pos_device(pos_id)


def pump_transactions(card_count, pos_count, tx_count, report_pct):
    # Create transactions. If the POS is compromised, then the
    # Card of the transaction gets compromised too. If the card
    # is compromised, there is a 0.1 chance the transaction is
    # fraudulent and detected (regardless of POS).
    q = ("MATCH (c:Card {{id: {}}}), (p:Pos {{id: {}}}) "
         "CREATE (t:Transaction "
         "{{id: {}, fraud_reported: c.compromised AND (rand() < %f)}}) "
         "CREATE (c)<-[:Using]-(t)-[:At]->(p) "
         "SET c.compromised = p.compromised;" % report_pct)

    def rint(max): return random.randint(0, max - 1)  # NOQA
    for i in range(tx_count):
        print(q.format(rint(card_count), rint(pos_count), i))


POS_COUNT = 1000
CARD_COUNT = 10000
FRAUD_POS_COUNT = 20 
TX_COUNT = 50000
REPORT_PCT = 0.1

random.seed(12345)

init_data(CARD_COUNT, POS_COUNT)
compromise_pos_devices(POS_COUNT, FRAUD_POS_COUNT)
pump_transactions(CARD_COUNT, POS_COUNT, TX_COUNT, REPORT_PCT)
