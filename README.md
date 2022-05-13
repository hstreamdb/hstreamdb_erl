# hstreamdb_erlang

An OTP library for [HStreamDB](https://hstream.io/).

Git HEAD EDoc: https://hstreamdb.github.io/hstreamdb-erlang/

[![.github/workflows/ci.yml](https://github.com/hstreamdb/hstreamdb-erlang/actions/workflows/ci.yml/badge.svg)](https://github.com/hstreamdb/hstreamdb-erlang/actions/workflows/ci.yml)

## Installation

### Rebar3

Add the following line to the `deps` field of the file `rebar.config`

```erl
{hstreamdb_erlang, {git, "git@github.com:hstreamdb/hstreamdb-erlang.git", {branch, "main"}}}
```

### Mix

Add the following line to the `deps` function of the file `mix.exs`

```exs
{:hstreamdb_erlang, git: "git@github.com:hstreamdb/hstreamdb-erlang.git", branch: "main"}
```

## Usage

Currently, all user functions are exposed through the

- module [hstreamdb_erlang](./src/hstreamdb_erlang.erl)
- module [hstreamdb_erlang_producer](./src/hstreamdb_erlang_producer.erl)

### Connect to HStreamDB

```erl
ServerUrl = "http://localhost:6570",
{ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl).
```

### Work with Streams

```erl
StreamName = "s",
ReplicationFactor = 3,
BacklogDuration = 60 * 30,
hstreamdb_erlang:create_stream(
    Channel, StreamName, ReplicationFactor, BacklogDuration
).

hstreamdb_erlang:list_streams(Channel).

hstreamdb_erlang:delete_stream(Channel, StreamName, #{
    ignoreNonExist => true,
    force => true
}).
```

### Write Data to a Stream

```erl
BatchSetting = hstreamdb_erlang_producer:build_batch_setting({record_count_limit, 3}),
StartArgs = #{
    producer_option => hstreamdb_erlang_producer:build_producer_option(
        ServerUrl, StreamName, self(), 16, BatchSetting
    )
},
{ok, Producer} = hstreamdb_erlang_producer:start_link(StartArgs).

Record = hstreamdb_erlang_producer:build_record(<<"this_is_a_binary_literal">>),
hstreamdb_erlang_producer:append(Producer, Record).

hstreamdb_erlang_producer:flush(Producer),
receive
    {record_ids, RecordIds} ->
        io:format(
            "RecordIds: ~p~n", [RecordIds]
        )
end.
```
