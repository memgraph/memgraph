#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

if [ ! -x "$(command -v jq)" ]; then
 sudo apt-get install -y jq
fi

if [ ! -x "$(command -v go)" ]; then
  sudo apt install -y golang-go
  # or, https://go.dev/doc/install
fi

go install sigs.k8s.io/kind@v0.24.0
echo "kind installed under $(go env GOPATH)/bin"
export PATH="$(go env GOPATH)/bin:$PATH"
kind --version

if [ ! -f "/usr/local/bin/kubectl" ]; then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl.sha256"
  echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
  sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi
kubectl version --client

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

if [ ! -f "/usr/local/bin/helm" ]; then
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
fi

helm repo add memgraph https://memgraph.github.io/helm-charts
helm repo update
helm repo list

# Last released mgconsole.
# rm -rf $SCRIPT_DIR/bin # To download it again.
MG_CONSOLE_VERSION="v1.7.0"
MG_CONSOLE_BINARY="$SCRIPT_DIR/bin/mgconsole"
if [ ! -f "$MG_CONSOLE_BINARY" ]; then
  mkdir -p "$SCRIPT_DIR/bin"
  curl -fL "https://download.memgraph.com/mgconsole/$MG_CONSOLE_VERSION/linux-$(uname -m)/mgconsole" \
    -o "$MG_CONSOLE_BINARY"
  chmod +x "$MG_CONSOLE_BINARY"
fi
if [ -x "$MG_CONSOLE_BINARY" ]; then
  echo "$("$MG_CONSOLE_BINARY" --version) available at $MG_CONSOLE_BINARY"
else
  echo "failed to download mgconsole"
fi

cd $SCRIPT_DIR/query_modules
mkdir -p dist
g++ -std=c++20 -fPIC -shared -I$SCRIPT_DIR/../cpp/memgraph/include -o dist/basic_cpp.so basic.cpp

rm $SCRIPT_DIR/get_helm.sh || true
rm $SCRIPT_DIR/kubectl || true
rm $SCRIPT_DIR/kubectl.sha256 || true
