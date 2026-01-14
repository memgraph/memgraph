# Installation setup

This is fairly simple installation setup (created for Ubuntu 20.04) to follow:


#### 1. [IF NEEDED] Install the newer version of CMake (Version 20.0+) since it is requirement for cuGraph CMakeLists.txt. Example shows CMake 3.22.

```
sudo apt remove --purge cmake
sudo apt install build-essential libssl-dev
wget https://github.com/Kitware/CMake/releases/download/v3.22.2/cmake-3.22.2.tar.gz
tar -zxvf cmake-3.22.2.tar.gz
cd cmake-3.22.2
./bootstrap
make
sudo make install
```

#### 2. [IF NEEDED] Install CUDA on your device. Example here shows CUDA 11.6.

```
sudo apt-get install linux-headers-$(uname -r)
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin
sudo mv cuda-ubuntu2004.pin /etc/apt/preferences.d/cuda-repository-pin-600
wget https://developer.download.nvidia.com/compute/cuda/11.6.1/local_installers/cuda-repo-ubuntu2004-11-6-local_11.6.1-510.47.03-1_amd64.deb
sudo dpkg -i cuda-repo-ubuntu2004-11-6-local_11.6.1-510.47.03-1_amd64.deb
sudo apt-key add /var/cuda-repo-ubuntu2004-11-6-local/7fa2af80.pub
sudo apt-get update
sudo apt-get -y install cuda
```

#### 3. Make sure that your CUDA home and LD_LIBRARY_PATH are correctly set.

```
export CUDA_HOME=/usr/local/cuda
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/cuda/lib64:/usr/local/cuda/extras/CUPTI/lib64
export PATH=$PATH:$CUDA_HOME/bin
```

#### 4. Install system dependencies

```
sudo apt install libblas-dev liblapack-dev libboost-all-dev
```

#### 5. Download and install # NCCL is also required (NVIDIA Developer Program registration # is required ->. NCCL could be installed from  https://github.com/NVIDIA/nccl.

```
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-ubuntu2004.pin

sudo mv cuda-ubuntu2004.pin /etc/apt/preferences.d/cuda-repository-pin-600

sudo apt-key adv --fetch-keys https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/7fa2af80.pub

sudo add-apt-repository "deb https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/ /"

sudo apt-get update
sudo apt install libnccl2=2.11.4-1+cuda11.6 libnccl-dev=2.11.4-1+cuda11.6
```

#### 6. To use CUDA and NVIDIA cuGraph code, compile with the different flags.

```
cmake -DMAGE_CUGRAPH_ENABLE=ON ..
make -j$(nproc)
```
