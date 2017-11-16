#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import sys  
import os
import tempfile
from tabulate import tabulate
from timeit import default_timer as timer

# hackish way to resuse existing start code
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + 
        "/../tests/macro_benchmark/")
from databases import *
from clients import *
from common import get_absolute_path

def main():
    path = get_absolute_path("benchmarking.conf", "config")
    tmp_dir = tempfile.TemporaryDirectory()

    SNAPSHOT_DIR_ARG = ["--snapshot-directory", tmp_dir.name]
    MAKE_SNAPSHOT_ARGS = ["--snapshot-on-exit"] + SNAPSHOT_DIR_ARG
    RECOVER_SNAPSHOT_ARGS = ["--snapshot-recover-on-startup"] + SNAPSHOT_DIR_ARG
    snapshot_memgraph = Memgraph(MAKE_SNAPSHOT_ARGS, path, 1)
    recover_memgraph = Memgraph(RECOVER_SNAPSHOT_ARGS, path, 1)
    client = QueryClient(None, 1)

    results = []
    for node_cnt in [10**6, 5*10**6, 10**7]:
        for edge_per_node in [0, 1, 3]:
            for prop_per_node in [0, 1]:
                snapshot_memgraph.start()
                properties = "{}".format(",".join(
                    ["p{}: 0".format(x) for x in range(prop_per_node)])) 
                client(["UNWIND RANGE(1, {}) AS _ CREATE ({{ {} }})".format(node_cnt, properties)], snapshot_memgraph)
                client(["UNWIND RANGE(1, {}) AS _ MATCH (n) CREATE (n)-[:l]->(n)"
                        .format(edge_per_node)], snapshot_memgraph)
                snapshot_memgraph.stop()

                # This waits for the snapshot to be recovered and then exits
                start = timer()
                recover_memgraph.start()
                recover_memgraph.stop()
                stop = timer()
                diff = stop - start

                snapshots = os.listdir(tmp_dir.name)
                assert len(snapshots) == 1

                snap_path = tmp_dir.name + "/" + snapshots[0]
                snap_size = round(os.path.getsize(snap_path) / 1024. / 1024., 2)
                os.remove(snap_path)

                edge_cnt = edge_per_node * node_cnt
                results.append((node_cnt, edge_cnt, prop_per_node, snap_size, diff))

    print(tabulate(tabular_data=results, headers=["Nodes", "Edges", 
        "Properties", "Snapshot size (MB)", "Elapsed time (s)"]))

if __name__ == "__main__":
    main()
