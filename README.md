hstreamdb_erlang
=====

An OTP library for HStreamDB

Build
-----

    $ rebar3 compile


Example usage:

```erl
readme() ->
    ServerUrl = "http://127.0.0.1:6570",
    StreamName = hstreamdb_erlang_utils:string_format("~s-~p", [
        "___v2_test___", erlang:time()
    ]),
    BatchSetting = hstreamdb_erlang_producer:build_batch_setting({record_count_limit, 3}),

    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    _ = hstreamdb_erlang:delete_stream(Channel, StreamName, #{
        ignoreNonExist => true,
        force => true
    }),
    ReplicationFactor = 3,
    BacklogDuration = 60 * 30,
    ok = hstreamdb_erlang:create_stream(
        Channel, StreamName, ReplicationFactor, BacklogDuration
    ),
    _ = hstreamdb_erlang:stop_client_channel(Channel),

    StartArgs = #{
        producer_option => hstreamdb_erlang_producer:build_producer_option(
            ServerUrl, StreamName, BatchSetting
        )
    },
    {ok, Producer} = hstreamdb_erlang_producer:start_link(StartArgs),

    io:format("StartArgs: ~p~n", [StartArgs]),

    RecordIds = lists:map(
        fun(_) ->
            Record = hstreamdb_erlang_producer:build_record(<<"_">>),
            hstreamdb_erlang_producer:append(Producer, Record)
        end,
        lists:seq(0, 100)
    ),

    hstreamdb_erlang_producer:flush(Producer),

    timer:sleep(1000),

    lists:foreach(
        fun({ok, FutureRecordId}) ->
            RecordId = rpc:yield(FutureRecordId),
            io:format("~p~n", [RecordId])
        end,
        RecordIds
    ),

    ok.
```
