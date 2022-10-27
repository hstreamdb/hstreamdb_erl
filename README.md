[![Run test case](https://github.com/hstreamdb/hstreamdb_erl/actions/workflows/run_tests.yaml/badge.svg)](https://github.com/hstreamdb/hstreamdb_erl/actions/workflows/run_tests.yaml)

# `hstreamdb_erl`

Erlang driver for [HStreamDB](https://hstream.io).

## Build

```bash
rebar3 compile
```

## Run tests locally

```
make -C ./.ci up
./rebar3 ct --name 'test@127.0.0.1' -v
./rebar3 cover
make -C ./.ci down
```
