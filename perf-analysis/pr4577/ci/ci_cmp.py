#!/usr/bin/env python3
"""ci_cmp.py <baseline.json> <experiment.json> [...] — signal/control medians + raw per-query table."""
import json
import statistics as st
import sys

SIG = [
    "arango/expansion_1_with_filter/",
    "arango/single_vertex_read/",
    "match/vertex_on_label_property_index/",
    "arango/single_edge_write/",
    "create/vertex/",
    "match/vertex_on_property/",
    "arango/shortest_path/",
    "match/pattern_short/",
    "arango/single_vertex_write/",
    "match/pattern_long/",
    "create/pattern/",
    "create/vertex_big/",
]
CTL = [
    "arango/expansion_2/",
    "arango/expansion_2_with_filter/",
    "arango/expansion_3/",
    "arango/neighbours_2/",
    "arango/neighbours_2_with_filter/",
    "arango/neighbours_2_with_data_and_filter/",
    "arango/shortest_path_with_filter/",
    "arango/allshortest_paths/",
]

base = json.load(open(sys.argv[1]))
for path in sys.argv[2:]:
    exp = json.load(open(path))
    a, b = base["qps"], exp["qps"]
    common = [k for k in a if k in b]
    r = {k: b[k] / a[k] - 1 for k in common}
    sig = [r[k] for k in common if any(k.startswith(p) for p in SIG)]
    ctl = [r[k] for k in common if any(k.startswith(p) for p in CTL)]
    print(
        f"## {exp['branch']} ({exp['sha'][:9]}, {exp['runner']}) vs {base['branch']} ({base['sha'][:9]}, {base['runner']})"
    )
    print(
        f"SIGNAL median {st.median(sig) * 100:+.1f}% ({sum(x > 0 for x in sig)}/{len(sig)} faster) | "
        f"CONTROL median {st.median(ctl) * 100:+.1f}% | signal-control {(st.median(sig) - st.median(ctl)) * 100:+.1f}%"
    )
    for k in common:
        tag = "S" if any(k.startswith(p) for p in SIG) else "C" if any(k.startswith(p) for p in CTL) else " "
        print(f"  {tag} {'/'.join(k.split('/')[:2]):<42}{a[k]:>10.1f}{b[k]:>10.1f}{r[k] * 100:>+7.1f}%")
