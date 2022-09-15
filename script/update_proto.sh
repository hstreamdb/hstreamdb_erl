#!/bin/bash

set -e

tag="$1"

curl "https://raw.githubusercontent.com/hstreamdb/protocol/${tag}/hstream.proto" \
    --output "protocol/hstreamdb_api.proto" --silent --location


