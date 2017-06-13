FROM ubuntu:16.04
# FROM ubuntu 16.04       # 130MB
# FROM phusion/baseimage  # 220MB
# FROM debian:jessie-slim # doesn't work because CXXABI_1.3.9 & GLIBCXX_3.4.21 not found
# FROM debian:jessie      # doesn't work because CXXABI_1.3.9 & GLIBCXX_3.4.21 not found

ENV MEMGRAPH_CONFIG /memgraph/config/memgraph.conf

ARG build_name

COPY ${build_name} /memgraph

WORKDIR /memgraph

ENTRYPOINT ["./memgraph"]
CMD [""]
