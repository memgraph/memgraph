#!/bin/bash
# k8s-only helpers. These are NOT used by the Docker smoke tests that run in CI
# (tests/smoke/test_single.bash) -> they live here instead of in
# tests/smoke/utils.bash so that the CI path stays free of kubectl/helm.
K8S_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SMOKE_DIR="$( cd "$K8S_DIR/.." && pwd )"
source "$SMOKE_DIR/utils.bash"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-smoke-release-testing}" # Keep in sync with k8s/init.bash.

kind_load_image() {
  # NOTE: Both `kind load docker-image` and `kind load image-archive` run
  #   ctr images import --all-platforms --digests
  # inside the node, which chokes on what `docker save` produces from Docker's
  # containerd image store:
  #   * multi-arch image whose other platforms were never pulled ->
  #     "ctr: content digest <other arch's manifest> not found"
  #   * image carrying an attestation manifest (daily/RC builds) ->
  #     "ctr: mismatched image rootfs and manifest layers"
  # Exporting a single platform and importing it with an explicit --platform
  # (so the attestation manifest, which has no platform, is skipped) works for
  # both. The import is done per node because that is what `kind load` does.
  __image="$1"
  __platform="$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')"
  __workdir="$(mktemp -d)"
  if ! docker save --platform "$__platform" "$__image" -o "$__workdir/image.tar" 2>/dev/null; then
    # Fallback for docker versions without `docker save --platform`.
    rm -rf "$__workdir"
    kind load docker-image "$__image" -n "$KIND_CLUSTER_NAME"
    return
  fi
  for __node in $(kind get nodes -n "$KIND_CLUSTER_NAME"); do
    echo "Loading $__image ($__platform) into $__node..."
    docker exec --privileged -i "$__node" \
      ctr --namespace=k8s.io images import --platform "$__platform" --snapshotter=overlayfs - \
      < "$__workdir/image.tar"
  done
  rm -rf "$__workdir"
}

wait_for_memgraph_coordinator() {
  __host=$1
  __port=$2
  __max_retries=${3:-100}
  __retries=0
  while ! echo "SHOW INSTANCE;" | $MEMGRAPH_CONSOLE_BINARY --host $__host --port $__port > /dev/null 2>&1; do
    sleep 0.3
    __retries=$((__retries+1))
    if [ "$__retries" -ge "$__max_retries" ]; then
      echo "wait_for_memgraph_coordinator: Reached max retries ($__max_retries) for $__host:$__port"
      return 1
    fi
  done
  return 0
}

wait_for_memgraph_main() {
  __host=$1
  __port=$2
  __max_retries=${3:-20}
  __retries=0
  while ! echo "SHOW REPLICATION ROLE;" | $MEMGRAPH_CONSOLE_BINARY --host $__host --port $__port --output-format=csv | python3 $SMOKE_DIR/validator.py validate_is_main > /dev/null 2>&1; do
    sleep 0.3
    __retries=$((__retries+1))
    if [ "$__retries" -ge "$__max_retries" ]; then
      echo "wait_for_memgraph_main: Reached max retries ($__max_retries) for $__host:$__port"
      return 1
    fi
  done
  return 0
}

with_kubectl_portforward() (
    local target=$1  # svc/foo, pod/bar, deployment/baz, …
    local map=$2     # 8080:80 or 8443 or 0.0.0.0:8080:80; several space
                     #     separated mappings are allowed, the first one is the
                     #     one that gets probed, e.g. "8003:7687 9002:9091"
    local probe="$3" # probe to test the target process, e.g. wait_for_xyz
    shift 4          # “--” + user commands; NOTE: Use an array of commands
                     #     inside a string: -- 'cmd1' 'cmd2' ...
                     #     because if you use ; or && to separate commands,
                     #     bash will treat that as one command + cleanup + the
                     #     rest.

    local log
    local pf_pid
    local retries=0
    local max_retries=5
    while true; do
        log=$(mktemp)
        # NOTE: $map is deliberately unquoted -> it may hold several mappings.
        kubectl port-forward "$target" $map >/dev/null 2>>"$log" &
        pf_pid=$!
        # NOTE: port-forward doesn't have built-in timeout + the target process
        # might take arbitrary time to initialize. -> The only way to know if
        # everything is right in the shortest amount of time is to inject the
        # target process probe as one of the required params.
        sleep 0.3
        if ! eval "$probe"; then
          kill -9 "$pf_pid" 2>/dev/null || true
          wait "$pf_pid" 2>/dev/null || true
          retries=$((retries+1))
          if [ $retries -ge $max_retries ]; then
            echo "kubectl port-forward failed after $max_retries attempts — see $log and inspect the target process" >&2
            exit 1
          fi
        else
          break
        fi
    done

    cleanup() {
        echo "calling port forward cleanup"
        kill -9 "$pf_pid" 2>/dev/null || true
        wait "$pf_pid" 2>/dev/null || true
    }
    trap cleanup EXIT INT TERM

    local lport=${map%%:*}              # leftmost number before the first ':'
    for _ in {1..50}; do                # ~10 s max (50×0.2 s)
        if nc -z 127.0.0.1 "$lport" 2>/dev/null; then break; fi
        if ! kill -0 "$pf_pid" 2>/dev/null; then
            echo "port-forward crashed — see $log" >&2
            exit 1
        fi
        sleep 0.2
    done

    for cmd in "$@"; do
      eval "$cmd"
    done
)
