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
./rebar3 ct --name 'test@127.0.0.1' -v -c
./rebar3 cover
make -C ./.ci down
```

## Use TLS

ref: [HStream docs](https://hstream.io/docs/en/latest/operation/security/overview.html)

```erl
start() ->
  _ = application:ensure_all_started(hstreamdb_erl),
  GrpcOpts =
    #{gun_opts =>
        #{transport => ssl,
          transport_opts =>
            [{verify, verify_peer},
             {cacertfile, ?WS_PATH ++ "root_ca.crt"},
             {certfile, ?WS_PATH ++ "client.crt"},
             {keyfile, ?WS_PATH ++ "client.key"}]}},
  Opts = #{url => ?SERVER_URL, rpc_options => GrpcOpts},
  {ok, Client} = hstreamdb_client:start(test_client, Opts),
  hstreamdb_client:echo(Client).
```
