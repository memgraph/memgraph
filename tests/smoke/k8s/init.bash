#!/bin/bash -e
# Installs the tooling the k8s smoke tests need (go, kind, kubectl, helm) and
# brings up a local kind cluster. This is only for the MANUAL k8s path
# (tests/smoke/test_k8s.bash) -> CI runs the Docker-only smoke tests and never
# calls this script.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR" # The downloads below land in the current directory.

KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-smoke-release-testing}" # Keep in sync with k8s/utils.bash.

if [ "$(uname -m)" == "x86_64" ]; then
  ARCH="amd64"
else
  ARCH="arm64"
fi

curl -L "https://go.dev/dl/go1.25.3.linux-$ARCH.tar.gz" -o go.tar.gz
mkdir -p $HOME/go-install
tar -xzf go.tar.gz -C $HOME/go-install
export PATH="$HOME/go-install/go/bin:$PATH"
go version

go install sigs.k8s.io/kind@v0.24.0
echo "kind installed under $(go env GOPATH)/bin"
export PATH="$(go env GOPATH)/bin:$PATH"
kind --version

if ! command -v kubectl > /dev/null 2>&1; then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$ARCH/kubectl"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$ARCH/kubectl.sha256"
  echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi
kubectl version --client

# delete any leftover cluster
kind delete cluster --name "$KIND_CLUSTER_NAME" || true

# Create cluster and wait for it to be ready
kubectl cluster-info --context "kind-$KIND_CLUSTER_NAME" > /dev/null 2>&1 \
  || {
       echo "Creating cluster..."
       kind create cluster --name "$KIND_CLUSTER_NAME" --wait 120s
       echo "...done"
     }

# Set kubectl context to use the kind cluster
kubectl config use-context "kind-$KIND_CLUSTER_NAME"

if ! command -v helm > /dev/null 2>&1; then
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
fi

helm repo add memgraph https://memgraph.github.io/helm-charts
helm repo update
helm repo list

rm -f "$SCRIPT_DIR/go.tar.gz" || true
rm -f "$SCRIPT_DIR/kubectl" || true
rm -f "$SCRIPT_DIR/kubectl.sha256" || true
rm -f "$SCRIPT_DIR/get_helm.sh" || true

echo "k8s smoke test prerequisites are ready."
echo "NOTE: kind is installed under $(go env GOPATH)/bin, put it on your PATH:"
echo "  export PATH=\"$(go env GOPATH)/bin:\$PATH\""
echo "Then run $SCRIPT_DIR/../test_k8s.bash"
