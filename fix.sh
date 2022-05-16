#!/usr/bin/env sh

rebar3 grpc gen && rm -rf ./src/hstream_server_h_stream_api_bhvr.erl && find . -type f -name '*.erl' -exec sed -i "s/HStreamApi:/'HStreamApi':/g" {} \;
