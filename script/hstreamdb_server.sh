#! /usr/bin/env bash

set -e

DEFAULT_HSTREAM_DOCKER_TAG="latest"

docker pull hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG}

mkdir /tmp/hstream-test-data

docker run \
    -td \
    --rm  \
    --name zookeeper-test \
    --network host \
    zookeeper:3.6

docker run \
    -td \
    --rm  \
    -v /tmp/hstream-test-data:/data/store \
    --name hstore-test \
    --network host \
    hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
    ld-dev-cluster --root /data/store --use-tcp

sleep 10

docker run \
  -td \
  -v /tmp/hstream-test-data:/data/store \
  --name hserver-test0 \
  --network host \
  -p 6570:6570 \
  hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
  hstream-server --port 6570 --store-config /data/store/logdevice.conf --server-id 0 --log-level debug --address ${your host: 127.0.0.1}

docker run \
  -td \
  -v /tmp/hstream-test-data:/data/store \
  --name hserver-test1 \
  --network host \
  -p 6571:6571 \
  hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
  hstream-server --port 6571 --store-config /data/store/logdevice.conf --server-id 1 --log-level debug --address ${your host: 127.0.0.1}

docker run \
  -td \
  -v /tmp/hstream-test-data:/data/store \
  --name hserver-test2 \
  --network host \
  -p 6572:6572 \
  hstreamdb/hstream:${DEFAULT_HSTREAM_DOCKER_TAG} \
  hstream-server --port 6572 --store-config /data/store/logdevice.conf --server-id 2 --log-level debug --address ${your host: 127.0.0.1}
