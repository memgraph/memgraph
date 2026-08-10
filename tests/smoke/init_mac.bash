#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

if [ ! -x "$(command -v go)" ]; then
  brew install go
fi
go version

if [ ! -f "$(go env GOPATH)/bin/kind" ]; then
  go install sigs.k8s.io/kind@v0.24.0
  echo "kind installed under $(go env GOPATH)/bin"
fi
export PATH="$(go env GOPATH)/bin:$PATH"
kind --version

if [ ! -f "/usr/local/bin/kubectl" ]; then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/darwin/arm64/kubectl"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/darwin/arm64/kubectl.sha256"
  echo "$(cat kubectl.sha256)  kubectl" | shasum -a 256 --check
  sudo install -o root -m 0755 kubectl /usr/local/bin/kubectl
fi
kubectl version --client

if [ ! -f "/usr/local/bin/helm" ]; then
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
fi
helm version

# Delete any leftover cluster
kind delete cluster --name smoke-release-testing || true
# Create cluster and wait for it to be ready
kubectl cluster-info --context kind-smoke-release-testing > /dev/null 2>&1 || \
  {
    echo "Creating cluster..."
    kind create cluster --name smoke-release-testing --wait 120s
    echo "...done"
  }
kubectl get all -A

helm repo add memgraph https://memgraph.github.io/helm-charts
helm repo update
helm repo list
# helm install my-release memgraph/memgraph # TODO: Fails if it's already there -> figure out how to skip.

# NOTE: Downloading the last released mgconsole (macos universal binary).
# rm -rf $SCRIPT_DIR/bin # To download it again.
MG_CONSOLE_VERSION="v1.7.0"
MG_CONSOLE_BINARY="$SCRIPT_DIR/bin/mgconsole"
if [ ! -f "$MG_CONSOLE_BINARY" ]; then
  mkdir -p "$SCRIPT_DIR/bin"
  curl -fL "https://download.memgraph.com/mgconsole/$MG_CONSOLE_VERSION/macos/mgconsole" \
    -o "$MG_CONSOLE_BINARY"
  chmod +x "$MG_CONSOLE_BINARY"
fi
if [ -x "$MG_CONSOLE_BINARY" ]; then
  echo "$("$MG_CONSOLE_BINARY" --version) available at $MG_CONSOLE_BINARY"
else
  echo "failed to download mgconsole"
fi

rm $SCRIPT_DIR/get_helm.sh || true
