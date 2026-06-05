#!/bin/bash
# Single source of truth for the version string of memgraph packages built
# from master. These are NOT versioned by release/get_version.py — master/daily
# packages are renamed to <latest-tag>+pr<pr>~<commit>. Both the "Rename Package"
# step (reusable_package.yaml) and the MAGE download-link resolver
# (daily_build.yml) consume this.
set -euo pipefail
# Which encoding of the (tag, pr, commit) tuple to emit. These mirror the
# distinct version conventions of each artifact type (cf. release/get_version.py):
#   binary|deb : tag+pr~commit         (memgraph_<v>-1_<arch>.deb)
#   rpm        : tag_0.pr.commit        (memgraph-<v>-1.<arch>.rpm)
#   docker     : tag_pr_commit          (memgraph-<v>[-relwithdebinfo]-docker.tar.gz)
variant="${1:-binary}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tag="$("$here/get_latest_tag.sh")"
commit_log="$(git log --pretty=%B)"
pr="$(grep -oPm1 '(?<=\(#)\d+(?=\))' <<<"$commit_log" || true)"
commit="$(git rev-parse HEAD | cut -c1-12)"
case "$variant" in
  rpm)    printf '%s_0.pr%s.%s' "$tag" "$pr" "$commit" ;;
  docker) printf '%s_pr%s_%s'   "$tag" "$pr" "$commit" ;;
  *)      printf '%s+pr%s~%s'   "$tag" "$pr" "$commit" ;;
esac
