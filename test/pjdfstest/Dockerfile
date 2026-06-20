ARG WEED_IMAGE=kmbae27/seaweedfs:latest
FROM ${WEED_IMAGE} AS weed

FROM debian:bookworm-slim

COPY --from=weed /usr/bin/weed /usr/local/bin/weed

RUN apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 update && \
    DEBIAN_FRONTEND=noninteractive apt-get -o Acquire::Retries=5 -o Acquire::http::Timeout=30 install -y \
    --no-install-recommends \
    --no-install-suggests \
    autoconf \
    automake \
    build-essential \
    ca-certificates \
    curl \
    fuse3 \
    git \
    libtap-harness-archive-perl \
    perl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ARG PJDFSTEST_REPO=https://github.com/pjd/pjdfstest.git
ARG PJDFSTEST_REF=ededbeb2b44929972898afb87474b0937f78a877

RUN git clone "${PJDFSTEST_REPO}" /opt/pjdfstest && \
    cd /opt/pjdfstest && \
    git checkout "${PJDFSTEST_REF}" && \
    autoreconf -ifs && \
    ./configure && \
    make pjdfstest

COPY run_inside_container.sh /run.sh
COPY known_failures.txt /known_failures.txt
RUN chmod +x /run.sh

ENTRYPOINT ["/usr/local/bin/weed"]
