#!/bin/bash -e
# Installs the tooling the k8s smoke tests need (kind, kubectl, helm) and brings
# up a local kind cluster. This is only for the MANUAL k8s path
# (tests/smoke/test_k8s.bash) -> CI runs the Docker-only smoke tests and never
# calls this script.
#
# NOTE: This does NOT source ../utils.bash on purpose -> installing tooling
# should not require the license/image environment variables.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-smoke-release-testing}" # Keep in sync with k8s/utils.bash.
KIND_VERSION="v0.24.0"

case "$(uname -s)" in
  Linux)  OS="linux"  ;;
  Darwin) OS="darwin" ;;
  *) echo "Unsupported OS: $(uname -s)"; exit 1 ;;
esac
if [ "$(uname -m)" == "x86_64" ]; then
  ARCH="amd64"
else
  ARCH="arm64"
fi

WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

if ! command -v kind > /dev/null 2>&1; then
  # NOTE: A released binary is used instead of `go install sigs.k8s.io/kind` to
  # avoid pulling in a whole Go toolchain just to get kind.
  curl -fL "https://kind.sigs.k8s.io/dl/$KIND_VERSION/kind-$OS-$ARCH" -o "$WORK_DIR/kind"
  sudo install -m 0755 "$WORK_DIR/kind" /usr/local/bin/kind
fi
kind --version

if ! command -v kubectl > /dev/null 2>&1; then
  KUBECTL_VERSION="$(curl -fL -s https://dl.k8s.io/release/stable.txt)"
  curl -fL "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$OS/$ARCH/kubectl" -o "$WORK_DIR/kubectl"
  curl -fL "https://dl.k8s.io/release/$KUBECTL_VERSION/bin/$OS/$ARCH/kubectl.sha256" -o "$WORK_DIR/kubectl.sha256"
  if [ "$OS" == "darwin" ]; then
    (cd "$WORK_DIR" && echo "$(cat kubectl.sha256)  kubectl" | shasum -a 256 --check)
  else
    (cd "$WORK_DIR" && echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check)
  fi
  sudo install -m 0755 "$WORK_DIR/kubectl" /usr/local/bin/kubectl
fi
kubectl version --client

if ! command -v helm > /dev/null 2>&1; then
  curl -fsSL -o "$WORK_DIR/get_helm.sh" https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 "$WORK_DIR/get_helm.sh"
  "$WORK_DIR/get_helm.sh"
fi
helm version

# Delete any leftover cluster.
kind delete cluster --name "$KIND_CLUSTER_NAME" || true
# Create cluster and wait for it to be ready.
kubectl cluster-info --context "kind-$KIND_CLUSTER_NAME" > /dev/null 2>&1 \
  || {
       echo "Creating cluster..."
       kind create cluster --name "$KIND_CLUSTER_NAME" --wait 120s
       echo "...done"
     }
# Set kubectl context to use the kind cluster.
kubectl config use-context "kind-$KIND_CLUSTER_NAME"
kubectl get all -A

# NOTE: It's critical to run `helm repo update` because otherwise an outdated
# template might get injected.
helm repo add memgraph https://memgraph.github.io/helm-charts
helm repo update
helm repo list

echo "k8s smoke test prerequisites are ready -> run $SCRIPT_DIR/../test_k8s.bash"
