#!/bin/bash
# Compile one translation unit with MG_ENTERPRISE removed, using its real build command.
# The community CI pass compiles out every enterprise branch, so code that only ever builds
# with MG_ENTERPRISE=ON can reference members that do not exist there.
#
#   tools/check-community-compile.sh src/query/interpreter.cpp
set -euo pipefail
FILE="${1:?usage: $0 <source file>}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$(mktemp -d)/probe.o"
CMD="$(python3 - "$ROOT/build/compile_commands.json" "$FILE" "$OUT" <<'PY'
import json, shlex, sys
db, want, out = json.load(open(sys.argv[1])), sys.argv[2], sys.argv[3]
for e in db:
    if e['file'].endswith(want):
        args, res, i = shlex.split(e.get('command') or ' '.join(e['arguments'])), [], 0
        while i < len(args):
            if args[i] == '-DMG_ENTERPRISE':
                i += 1
            elif args[i] == '-o':
                res += ['-o', out]; i += 2
            else:
                res.append(args[i]); i += 1
        print(' '.join(shlex.quote(a) for a in res))
        break
else:
    sys.exit(f"no compile command for {want}")
PY
)"
source "$ROOT/build/generators/conanbuild.sh" 2>/dev/null || true
cd "$ROOT/build" && eval "$CMD"
echo "community compile OK: $FILE"
