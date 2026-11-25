ARG RAPIDS_VERSION=26.10
ARG CUDA_VERSION=13
# >= 13.4: RAPIDS 26.10's libcuvs needs nvJitLink 13.4 (see the COPY below).
ARG CUDA_VERSION_MINOR=13.4.1
ARG PY_VERSION=3.12

FROM nvcr.io/nvidia/rapidsai/base:${RAPIDS_VERSION}-cuda${CUDA_VERSION}-py${PY_VERSION} AS cugraph-dev

FROM nvidia/cuda:${CUDA_VERSION_MINOR}-devel-ubuntu24.04 AS dev

# The MAGE modules link libcugraph.so only; the rest is its runtime closure in
# RAPIDS 26.10 (cugraph_common, cuvs, kvikio and nccl are new DT_NEEDED entries
# since 25.12). libcugraph_c.so is not linked and would drag in the 300 MB
# multi-GPU libcugraph_mg.so, so it is left out. The CUDA toolkit libraries
# (cublas, cusparse, curand, cusolver, nvrtc, nvJitLink) come from the
# nvidia/cuda base image, which must be >= 13.4: libcuvs needs nvJitLink 13.4.
# libcurl4 and libgomp1 come from apt.
COPY --from=cugraph-dev \
  /opt/conda/lib/libcugraph.so \
  /opt/conda/lib/libcugraph_common.so \
  /opt/conda/lib/libcuvs.so \
  /opt/conda/lib/libkvikio.so \
  /opt/conda/lib/libnccl.so.2 \
  /opt/conda/lib/librmm.so \
  /opt/conda/lib/librapids_logger.so \
  /opt/conda/lib/
COPY --from=cugraph-dev /opt/conda/include /opt/conda/include

ARG TOOLCHAIN_VERSION

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y --no-install-recommends \
    ca-certificates wget git libcurl4t64 libgomp1
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q --max-redirect=0 https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-${TOOLCHAIN_VERSION}/toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz \
    -O toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz \
    && tar xzvf toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz -C /opt \
    && rm toolchain-${TOOLCHAIN_VERSION}-binaries-x86_64.tar.gz

# Install toolchain run deps and memgraph build deps
ARG GIT_REF
SHELL ["/bin/bash", "-c"]
RUN git clone --branch ${GIT_REF} https://github.com/memgraph/memgraph.git \
    && cd memgraph \
    && if ! grep -q '^MG_NVM_COMMIT=' environment/util.sh; then \
         echo "environment/util.sh at ${GIT_REF} predates the pinned rust/node installers" >&2; \
         exit 1; \
       fi \
    && ./environment/os/ubuntu-24.04.sh install TOOLCHAIN_RUN_DEPS \
    && ./environment/os/ubuntu-24.04.sh install MEMGRAPH_BUILD_DEPS \
    && ./environment/os/ubuntu-24.04.sh install MEMGRAPH_TEST_DEPS \
    && ./environment/os/ubuntu-24.04.sh install MEMGRAPH_RUN_DEPS \
    && cp environment/util.sh /tmp/util.sh \
    && cd .. && rm -rf memgraph

# Create mg user and set as default
RUN useradd -m -s /bin/bash mg \
    && chown mg: /tmp/util.sh
USER mg

ENV LD_LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64:/usr/local/lib:/opt/conda/lib
ENV PATH=/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV CXXFLAGS="-DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE"
ENV CUDAFLAGS="-DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE"

# Install rust and node for mg.
ARG RUST_VERSION
ARG NODE_VERSION
# util.sh falls back to its own MG_*_VERSION defaults when these are empty.
RUN export MG_RUST_VERSION="${RUST_VERSION}" MG_NODE_VERSION="${NODE_VERSION}" \
    && source /tmp/util.sh \
    && retry_install install_rust "$MG_RUST_VERSION" \
    && retry_install install_node "$MG_NODE_VERSION" \
    && rm /tmp/util.sh

ENTRYPOINT ["sleep", "infinity"]
