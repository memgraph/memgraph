#!/usr/bin/env bash
# Usage: ci_bench.sh <branch>   — benchmark-only Diff run on <branch>, cancelled once mgbench is done.
# Writes <branch-slug>.log and <branch-slug>.json (query -> QPS) next to this script. Needs gh + write access to the repo.
set -euo pipefail
B=$1; R=memgraph/memgraph; HERE=$(cd "$(dirname "$0")" && pwd); SLUG=${B//\//_}
OFF=""; for i in community_core coverage_core coverage_clang_tidy debug_core debug_integration jepsen_core release_core release_stress mage_amd mage_arm mage_cuda mgcxx_unit; do OFF="$OFF -f $i=false"; done
SHA=$(git ls-remote https://github.com/$R.git "refs/heads/$B" | cut -f1)
before=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gh workflow run diff.yaml -R $R --ref "$B" $OFF -f release_benchmark=true
RUN=""; until [ -n "$RUN" ]; do sleep 10
  RUN=$(gh run list -R $R --workflow diff.yaml --branch "$B" --event workflow_dispatch -L 5 --json databaseId,createdAt,headSha \
        -q "[.[] | select(.createdAt >= \"$before\" and .headSha == \"$SHA\")][0].databaseId // empty"); done
echo "run $RUN https://github.com/$R/actions/runs/$RUN"
JOB=""; until [ -n "$JOB" ]; do sleep 20
  JOB=$(gh api "repos/$R/actions/runs/$RUN/jobs?per_page=100" -q '.jobs[] | select(.name=="Release / Benchmark") | .id // empty'); done
while :; do
  st=$(gh api "repos/$R/actions/jobs/$JOB" -q '(.steps[] | select(.name=="Run mgbench") | .conclusion) // "pending"' 2>/dev/null || echo pending)
  js=$(gh api "repos/$R/actions/jobs/$JOB" -q .status)
  [ "$js" = completed ] && break
  if [ "$st" = success ] || [ "$st" = failure ]; then gh run cancel -R $R "$RUN" >/dev/null || true; fi
  sleep 60
done
until [ "$(gh run view -R $R "$RUN" --json status -q .status)" = completed ]; do sleep 20; done
runner=$(gh api "repos/$R/actions/jobs/$JOB" -q .runner_name)
gh api "repos/$R/actions/jobs/$JOB/logs" > "$HERE/$SLUG.log"
python3 - "$HERE/$SLUG.log" "$HERE/$SLUG.json" "$B" "$SHA" "$RUN" "$runner" "$HERE" <<'EOF'
import sys, json; sys.path.insert(0, sys.argv[7]); import bench_compare as b
q, _, _ = b.parse_pr_log(open(sys.argv[1]).read())
json.dump({"branch": sys.argv[3], "sha": sys.argv[4], "run": sys.argv[5], "runner": sys.argv[6], "qps": q}, open(sys.argv[2], "w"), indent=1)
print(f"{sys.argv[3]} @ {sys.argv[4][:9]} on {sys.argv[6]}: {len(q)} mgbench queries")
EOF
