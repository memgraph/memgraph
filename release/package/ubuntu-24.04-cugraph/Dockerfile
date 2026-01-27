ARG RAPIDS_VERSION=25.12
ARG CUDA_VERSION=13
ARG CUDA_VERSION_MINOR=13.1.0
ARG PY_VERSION=3.12

FROM nvcr.io/nvidia/rapidsai/base:${RAPIDS_VERSION}-cuda${CUDA_VERSION}-py${PY_VERSION} AS cugraph-dev

FROM nvidia/cuda:${CUDA_VERSION_MINOR}-devel-ubuntu24.04 AS dev

COPY --from=cugraph-dev /opt/conda/lib/libcugraph.so /opt/conda/lib/libcugraph.so
COPY --from=cugraph-dev /opt/conda/lib/libcugraph_c.so /opt/conda/lib/libcugraph_c.so
COPY --from=cugraph-dev /opt/conda/lib/librmm.so /opt/conda/lib/librmm.so
COPY --from=cugraph-dev /opt/conda/lib/librapids_logger.so /opt/conda/lib/librapids_logger.so
COPY --from=cugraph-dev /opt/conda/include /opt/conda/include

ARG TOOLCHAIN_VERSION

# Stops tzdata interactive configuration.
ENV DEBIAN_FRONTEND=noninteractive

RUN apt update && apt install -y \
    ca-certificates wget git
# Do NOT be smart here and clean the cache because the container is used in the
# stateful context.

RUN wget -q https://s3-eu-west-1.amazonaws.com/deps.memgraph.io/toolchain-${TOOLCHAIN_VERSION}/toolchain-${TOOLCHAIN_VERSION}-binaries-ubuntu-24.04-amd64.tar.gz \
    -O toolchain-${TOOLCHAIN_VERSION}-binaries-ubuntu-24.04-amd64.tar.gz \
    && tar xzvf toolchain-${TOOLCHAIN_VERSION}-binaries-ubuntu-24.04-amd64.tar.gz -C /opt \
    && rm toolchain-${TOOLCHAIN_VERSION}-binaries-ubuntu-24.04-amd64.tar.gz

# Install toolchain run deps and memgraph build deps
ARG GIT_REF
SHELL ["/bin/bash", "-c"]
RUN git clone --branch ${GIT_REF} https://github.com/memgraph/memgraph.git \
    && cd memgraph \
    && ./environment/os/ubuntu-24.04.sh install TOOLCHAIN_RUN_DEPS \
    && ./environment/os/ubuntu-24.04.sh install MEMGRAPH_BUILD_DEPS \
    && cd .. && rm -rf memgraph

# Create mg user and set as default
RUN useradd -m -s /bin/bash mg
USER mg

ENV LD_LIBRARY_PATH=/usr/local/nvidia/lib:/usr/local/nvidia/lib64:/usr/local/cuda/lib64:/usr/local/lib:/opt/conda/lib
ENV PATH=/usr/local/nvidia/bin:/usr/local/cuda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENV CXXFLAGS="-DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE"
ENV CUDAFLAGS="-DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE"

# Install rust
ARG RUST_VERSION
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
    && . "$HOME/.cargo/env" \
    && rustup default ${RUST_VERSION}

# Fix node
ARG NODE_VERSION
RUN curl https://raw.githubusercontent.com/creationix/nvm/master/install.sh | bash \
    && . ~/.nvm/nvm.sh \
    && nvm install ${NODE_VERSION} \
    && nvm use ${NODE_VERSION}

ENTRYPOINT ["sleep", "infinity"]
