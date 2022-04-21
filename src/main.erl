-module(main).

-export([bench/0]).
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

    Countdown = spawn(fun() -> countdown(length(Producers), SelfPid) end),

    Append = fun(Producer, Record) ->
        {ok, FutureRecordId} =
            try
                hstreamdb_erlang_producer:append(Producer, Record)
            catch
                _:_ -> FailedAppendsIncr()
            end,
        FutureRecordId
    end,

    Record = hstreamdb_erlang_producer:build_record(Payload),
    lists:foreach(
        fun(Producer) ->
            spawn(fun() ->
                FutureRecordIds = lists:map(
                    fun(_) ->
                        Append(Producer, Record)
                    end,
                    lists:seq(1, 1000)
                ),
                hstreamdb_erlang_producer:flush(Producer),
                lists:foreach(
                    fun(X) ->
                        rpc:yield(X),
                        SuccessAppendsIncr()
                    end,
                    FutureRecordIds
                ),
                Countdown ! finished
            end)
        end,
        Producers
    ),

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

countdown(N, Pid) ->
    case N of
        0 ->
            Pid ! finished;
        _ ->
            receive
                finished ->
                    countdown(N - 1, Pid)
            end
    end.

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
