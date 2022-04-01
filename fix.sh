#!/usr/bin/env sh

rebar3 grpc gen && find . -type f -name '*.erl' -exec sed -i "s/HStreamApi:/'HStreamApi':/g" {} \;
