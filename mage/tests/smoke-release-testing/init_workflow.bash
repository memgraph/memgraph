#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "$SCRIPT_DIR/utils.bash"

if [ "$(arch)" == "x86_64" ]; then
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

if !command -v kubectl > /dev/null 2>&1; then
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$ARCH/kubectl"
  curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$ARCH/kubectl.sha256"
  echo "$(cat kubectl.sha256)  kubectl" | sha256sum --check
  install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi
kubectl version --client

# delete any leftover cluster
kind delete cluster --name smoke-release-testing || true

# Create cluster and wait for it to be ready
kubectl cluster-info --context kind-smoke-release-testing > /dev/null 2>&1 \
  || {
       echo "Creating cluster..."
       kind create cluster --name smoke-release-testing --wait 120s
       echo "...done"
     }

# Set kubectl context to use the kind cluster
kubectl config use-context kind-smoke-release-testing

if !command -v helm > /dev/null 2>&1; then
  curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
  chmod 700 get_helm.sh
  ./get_helm.sh
fi

helm repo add memgraph https://memgraph.github.io/helm-charts
helm repo update
helm repo list

# Last mgconsole.
# rm -rf $SCRIPT_DIR/mgconsole.build # To download and rebuild everything.
if [ ! -d "$SCRIPT_DIR/mgconsole.build" ]; then
  git clone https://github.com/memgraph/mgconsole.git "$SCRIPT_DIR/mgconsole.build"
fi
MG_CONSOLE_TAG="master"
MG_CONSOLE_BINARY="$SCRIPT_DIR/mgconsole.build/build/src/mgconsole"
if [ ! -f "$MG_CONSOLE_BINARY" ]; then
  cd "$SCRIPT_DIR/mgconsole.build"
  git checkout $MG_CONSOLE_TAG
  mkdir -p build && cd build
  cmake -DCMAKE_RELEASE_TYPE=Release ..
  make -j8
fi
if [ -x "$MG_CONSOLE_BINARY" ]; then
  echo "mgconsole available"
else
  echo "failed to build mgconsole"
fi

cd "$SCRIPT_DIR/query_modules"
mkdir -p dist
g++ -std=c++20 -fPIC -shared -I"$SCRIPT_DIR/../cpp/memgraph/include" -o dist/basic_cpp.so basic.cpp

rm "$SCRIPT_DIR/get_helm.sh" || true
rm "$SCRIPT_DIR/kubectl" || true
rm "$SCRIPT_DIR/kubectl.sha256" || true
