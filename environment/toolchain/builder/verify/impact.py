#!/usr/bin/env python3
"""What changing one stage costs, as the build runs and as the graph allows.

Bumping a tool rebuilds it and everything downstream. Because the stages run in
one line, downstream currently means everything after it, whether or not it
needs it. The manifest says what actually needs what, so the two numbers can be
compared -- the gap is what fanning the graph out would be worth.

Run: verify/impact.py [stage]   (or `just impact llvm`; no argument lists all)
"""
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent

_spec = importlib.util.spec_from_file_location("stage_graph", ROOT / "verify" / "stage-graph.py")
_sg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_sg)

manifest, _built, order = _sg.load()


def needs(stage):
    d = manifest[stage]["deps"]
    if d == ["*"]:
        return set(order[: order.index(stage)])
    return set(d)


def downstream(start):
    """Stages that would have to rebuild, following declared dependencies."""
    out, changed = {start}, True
    while changed:
        changed = False
        for s in order:
            if s in out:
                continue
            if needs(s) & out:
                out.add(s)
                changed = True
    return out


def report(stage):
    chain = len(order) - order.index(stage)
    dag = len(downstream(stage))
    print(
        "%-18s chain %2d/%d   graph %2d/%d   %s"
        % (stage, chain, len(order), dag, len(order), "" if chain == dag else "%d fewer if fanned out" % (chain - dag))
    )


if len(sys.argv) > 1:
    name = sys.argv[1]
    if name not in manifest:
        print("no stage %r; try one of: %s" % (name, " ".join(order)), file=sys.stderr)
        sys.exit(2)
    report(name)
    print("\nwould rebuild, following dependencies:")
    print("  " + " ".join(s for s in order if s in downstream(name)))
else:
    for s in order:
        report(s)
    ch = sum(len(order) - i for i in range(len(order)))
    dg = sum(len(downstream(s)) for s in order)
    n = len(order)
    print("\nmean rebuilt per change: %.1f as chained, %.1f as a graph" % (ch / n, dg / n))
