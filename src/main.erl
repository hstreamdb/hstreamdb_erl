-module(main).

-export([bench/0, readme/0]).
-export([remove_all_streams/1]).

remove_all_streams(Channel) ->
    {ok, Streams} = hstreamdb_erlang:list_streams(Channel),
    lists:foreach(
        fun(Stream) ->
            ok = hstreamdb_erlang:delete_stream(Channel, Stream, #{
                ignoreNonExist => true, force => true
            })
        end,
        lists:map(fun(Stream) -> maps:get(streamName, Stream) end, Streams)
    ).

bench(Opts) ->
    io:format("Opts: ~p~n", [Opts]),
    #{
        producerNum := ProducerNum,
        payloadSize := PayloadSize,
        serverUrl := ServerUrl,
        replicationFactor := ReplicationFactor,
        backlogDuration := BacklogDuration,
        reportIntervalSeconds := ReportIntervalSeconds,
        batchSetting := BatchSetting
    } =
        Opts,
    BatchNum = maps:get(record_count_limit, BatchSetting),
    Payload = get_bytes(PayloadSize),
    SelfPid = self(),

    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    Producers = lists:map(
        fun(X) ->
            StreamName =
                hstreamdb_erlang_utils:string_format(
                    "test_stream-~p-~p-~p",
                    [X, erlang:system_time(second), erlang:unique_integer()]
                ),
            ok =
                hstreamdb_erlang:create_stream(
                    Channel,
                    StreamName,
                    ReplicationFactor,
                    BacklogDuration
                ),
            ProducerOption = hstreamdb_erlang_producer:build_producer_option(
                ServerUrl, StreamName, BatchSetting
            ),
            ProducerStartArgs = hstreamdb_erlang_producer:build_start_args(ProducerOption),
            {ok, Producer} = hstreamdb_erlang_producer:start_link(ProducerStartArgs),
            Producer
        end,
        lists:seq(1, ProducerNum)
    ),

    SuccessAppends = atomics:new(1, [{signed, false}]),
    FailedAppends = atomics:new(1, [{signed, false}]),
    SuccessAppendsIncr = fun() -> atomics:add(SuccessAppends, 1, 1) end,
    FailedAppendsIncr = fun() -> atomics:add(FailedAppends, 1, 1) end,
    SuccessAppendsGet = fun() -> atomics:get(SuccessAppends, 1) end,
    FailedAppendsGet = fun() -> atomics:get(FailedAppends, 1) end,
    LastSuccessAppends = atomics:new(1, [{signed, false}]),
    LastFailedAppends = atomics:new(1, [{signed, false}]),
    LastSuccessAppendsPut = fun(X) -> atomics:put(LastSuccessAppends, 1, X) end,
    LastFailedAppendsPut = fun(X) -> atomics:put(LastFailedAppends, 1, X) end,
    LastSuccessAppendsGet = fun() -> atomics:get(LastSuccessAppends, 1) end,
    LastFailedAppendsGet = fun() -> atomics:get(LastFailedAppends, 1) end,

    Countdown = hstreamdb_erlang_utils:countdown(length(Producers), SelfPid),

    Append = fun(Producer, Record) ->
        {ok, FutureRecordId} =
            try
                hstreamdb_erlang_producer:append(Producer, Record)
            catch
                _:_ = E ->
                    logger:error("yield error: ~p~n", [E]),
                    FailedAppendsIncr()
            end,
        FutureRecordId
    end,

    ReportLoop =
        spawn(fun ReportLoop() ->
            LastSuccessAppendsPut(SuccessAppendsGet()),
            LastFailedAppendsPut(FailedAppendsGet()),
            timer:sleep(ReportIntervalSeconds * 1000),

            ReportSuccessAppends =
                (SuccessAppendsGet() - LastSuccessAppendsGet()) / ReportIntervalSeconds,
            ReportFailedAppends =
                (FailedAppendsGet() - LastFailedAppendsGet()) / ReportIntervalSeconds,
            ReportThroughput = ReportSuccessAppends * (PayloadSize * BatchNum) / 1024,
            io:format(
                "[BENCH]: SuccessAppends=~p, FailedAppends=~p, throughput=~p~n",
                [ReportSuccessAppends, ReportFailedAppends, ReportThroughput]
            ),
            ReportLoop()
        end),

    Record = hstreamdb_erlang_producer:build_record(Payload),
    lists:foreach(
        fun(Producer) ->
            spawn(fun() ->
                FutureRecordIds = lists:map(
                    fun(_) ->
                        Ret = Append(Producer, Record),
                        SuccessAppendsIncr(),
                        Ret
                    end,
                    lists:seq(1, 8000)
                ),
                hstreamdb_erlang_producer:flush(Producer),
                lists:foreach(
                    fun(X) ->
                        try
                            rpc:yield(X)
                        catch
                            _:_ -> logger:error("yield error: ~p~n", [X])
                        end
                    end,
                    FutureRecordIds
                ),
                Countdown ! finished
            end)
        end,
        Producers
    ),

    receive
        finished ->
            exit(ReportLoop, finished)
    end,

    % TODO: clean up
    ok.

bench() ->
    bench(#{
        producerNum => 100,
        payloadSize => 1,
        serverUrl => "http://127.0.0.1:6570",
        replicationFactor => 1,
        backlogDuration => 60 * 30,
        reportIntervalSeconds => 3,
        batchSetting => hstreamdb_erlang_producer:build_batch_setting({record_count_limit, 400})
    }).

bit_size_128() ->
    <<"___hstream.io___">>.

get_bytes(Size) ->
    get_bytes(Size, k).

get_bytes(Size, Unit) ->
    SizeBytes =
        case Unit of
            k ->
                Size * 1024;
            m ->
                Size * 1024 * 1024
        end,
    lists:foldl(
        fun(X, Acc) -> <<X/binary, Acc/binary>> end,
        <<"">>,
        lists:duplicate(round(SizeBytes / 128), bit_size_128())
    ).

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
