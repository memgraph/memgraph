#!/bin/bash -e
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Sources:
#   * https://medium.com/@subhampradhan966/kubeadm-setup-for-ubuntu-24-04-lts-f6a5fc67f0df
#   * https://chatgpt.com/share/6812882c-f450-800a-b13c-04eefccc0880

install_requirements() {
  ## SWAP
  # This is super important, kubelet doesn't want to get started.
  sudo swapoff -a
  # sudo sed -i '/ swap / s/^/#/' /etc/fstab
  ## BASICS
  sudo apt update -y
  sudo apt install -y apt-transport-https ca-certificates curl bridge-utils
  ## CONTAINERD
  sudo apt install -y containerd
  sudo mkdir -p /etc/containerd
  containerd config default | sudo tee /etc/containerd/config.toml > /dev/null
  sudo sed -i 's/SystemdCgroup = false/SystemdCgroup = true/' /etc/containerd/config.toml
  sudo systemctl restart containerd
  ## K8S BINARIES
  curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.30/deb/Release.key | sudo gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg
  echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.30/deb/ /' | sudo tee /etc/apt/sources.list.d/kubernetes.list
  sudo apt update
  sudo apt install -y kubelet kubeadm kubectl
  sudo apt-mark hold kubelet kubeadm kubectl
}

case $1 in
  master)
    sudo kubeadm reset -f
    rm -rf $HOME/.kube
    echo "Configuring master..."
    sudo kubeadm init --apiserver-advertise-address=100.94.19.81 --pod-network-cidr=10.244.0.0/16
    mkdir -p $HOME/.kube
    sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
    sudo chown $(id -u):$(id -g) $HOME/.kube/config
    # NOTE: The latest version of flannel doesn't work.
    # kubectl apply -f https://github.com/flannel-io/flannel/releases/latest/download/kube-flannel.yml
    curl -O https://raw.githubusercontent.com/flannel-io/flannel/v0.25.6/Documentation/kube-flannel.yml
    kubectl apply -f kube-flannel.yml
    kubeadm token create --print-join-command
  ;;

  worker)
    echo "Configuring worker..."
    sudo kubeadm reset -f
    # # Changing the node IP address from the control plane perspective -> doesn't actually work..
    # sudo mkdir -p /etc/systemd/system/kubelet.service.d
    # sudo vim /etc/systemd/system/kubelet.service.d/20-node-ip.conf
    # # [Service]
    # # Environment="KUBELET_EXTRA_ARGS=--node-ip=<your-desired-ip>"
    # sudo systemctl daemon-reexec
    # sudo systemctl daemon-reload
    # sudo systemctl restart kubelet
    sudo sysctl -w net.ipv4.ip_forward=1
    sudo kubeadm join --config $SCRIPT_DIR/worker-config.yaml
    # # To fix the "failed to set bridge addr cni0 alreadh has an ip addres"
    # ip link set cni0 down
    # brctl delbr cni0
  ;;

  *)
    echo "Only allowed options are master or worker."
    exit 1
  ;;
esac
