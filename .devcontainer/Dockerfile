# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

USER root
COPY .devcontainer/.host-ca/ca-certificates.crt /tmp/host-ca-certificates.crt
RUN test -s /tmp/host-ca-certificates.crt \
    && install -m 0644 /tmp/host-ca-certificates.crt /etc/ssl/certs/ca-certificates.crt \
    && rm /tmp/host-ca-certificates.crt
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        clang \
        cmake \
        musl-tools \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*
USER vscode
