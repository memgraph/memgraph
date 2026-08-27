#!/usr/bin/env python3
"""Check the Dockerfile matches the stage graph declared in stages/manifest.

The manifest is where a stage is described: what it needs, what it copies
beyond its own recipe and version file, and whether it gets the download cache.
The Dockerfile is one particular way of running that graph. Nothing generates
one from the other yet, so this checks they agree.

Order is checked as a constraint rather than as a fixed sequence: every stage
must run after everything it depends on. Two stages that do not depend on each
other may be in either order, because that is what it means for them not to
depend on each other -- and it is the difference between this and a hand-kept
list of positions, which called a harmless swap an error.

Run: verify/stage-graph.py   (or `just check`)
"""
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent


def parse_manifest(text):
    stages = {}
    for raw in text.splitlines():
        line = raw.split("#")[0].strip() if raw.strip().startswith("#") else raw.strip()
        if not line:
            continue
        parts = line.split()
        name, deps, extra, mount = parts[0], [], [], False
        for tok in parts[1:]:
            if tok == "@archives":
                mount = True
            elif tok.startswith("+"):
                extra.append(tok[1:])
            else:
                deps.append(tok)
        stages[name] = {"deps": deps, "extra": extra, "mount": mount}
    return stages


def parse_dockerfile(text):
    stages, order, cur = {}, [], None
    for line in text.splitlines():
        m = re.match(r"^FROM\s+\S+\s+AS\s+s-(\S+)", line)
        if m:
            cur = m.group(1)
            order.append(cur)
            stages[cur] = {"extra": [], "mount": False}
            continue
        if line.startswith("FROM "):
            cur = None
        if not cur:
            continue
        if line.startswith("COPY "):
            for tok in line.split()[1:-1]:
                if tok.startswith("--") or tok.startswith("stages/"):
                    continue
                if tok == "resolved/%s.env" % cur:
                    continue
                stages[cur]["extra"].append(tok)
        elif line.startswith("RUN ") and "type=cache" in line:
            stages[cur]["mount"] = True
    return stages, order


def load():
    """The declared graph, and the order the Dockerfile actually runs it in."""
    manifest = parse_manifest((ROOT / "stages" / "manifest").read_text())
    built, order = parse_dockerfile((ROOT / "Dockerfile").read_text())
    return manifest, built, order


def main():
    manifest, built, order = load()
    problems = []

    for s in order:
        if s not in manifest:
            problems.append("s-%s is built but not in the manifest" % s)
    for s in manifest:
        if s not in built:
            problems.append("%s is in the manifest but nothing builds it" % s)

    if not problems:
        seen = set()
        for s in order:
            deps = manifest[s]["deps"]
            if deps == ["*"]:
                missing = [d for d in order[: order.index(s)] if d not in seen]
            else:
                missing = [d for d in deps if d not in seen]
            for d in missing:
                problems.append("s-%s runs before %s, which it depends on" % (s, d))
            seen.add(s)

        for s in order:
            want, got = sorted(manifest[s]["extra"]), sorted(built[s]["extra"])
            if want != got:
                problems.append(
                    "s-%s copies %s, manifest says %s" % (s, " ".join(got) or "nothing", " ".join(want) or "nothing")
                )
            if manifest[s]["mount"] != built[s]["mount"]:
                said = "does" if manifest[s]["mount"] else "does not"
                problems.append("manifest says s-%s %s use the download cache" % (s, said))

    for p in problems:
        print("  %s" % p)
    if problems:
        sys.exit(1)
    print("%d stages match the manifest, and each runs after what it needs" % len(order))


if __name__ == "__main__":
    main()
