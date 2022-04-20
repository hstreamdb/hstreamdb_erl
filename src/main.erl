-module(main).

-export([bench/0]).

remove_all_streams(Client) ->
    {ok, Streams} = hstreamdb_erlang:list_streams(Client),
    lists:foreach(
        fun(Stream) ->
            ok = hstreamdb_erlang:delete_stream(Client, Stream, #{force => true})
        end,
        lists:map(fun(Stream) -> maps:get(streamName, Stream) end, Streams)
    ).

bench(Opts) ->
    io:format("Opts: ~p~n", [Opts]),
    #{
        producerNum := ProducerNum,
        payloadSize := PayloadSize,
        batchNum := BatchNum,
        serverUrl := ServerUrl,
        replicationFactor := ReplicationFactor,
        backlogDuration := BacklogDuration,
        reportIntervalSeconds := ReportIntervalSeconds
    } =
        Opts,

    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    remove_all_streams(Channel),

    XS = lists:map(
        fun(X) ->
            StreamName =
                hstreamdb_erlang_utils:string_format(
                    "test_stream-~p-~p",
                    [X, erlang:system_time(second)]
                ),
            {ok, Client} = hstreamdb_erlang:start_client_channel(ServerUrl),
            ok =
                hstreamdb_erlang:create_stream(
                    Client,
                    StreamName,
                    ReplicationFactor,
                    BacklogDuration
                ),
            {Client, StreamName}
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

    Append =
        fun({Client, StreamName}, X) ->
            case hstreamdb_erlang:append(Client, StreamName, "", raw, X) of
                {ok, _} -> SuccessAppendsIncr();
                {err, _} -> FailedAppendsIncr()
            end
        end,

    Payload = lists:duplicate(BatchNum, get_bytes(PayloadSize)),

    Pid = self(),
    Countdown = spawn(fun() -> countdown(length(XS), Pid) end),

    lists:foreach(
        fun(X) ->
            spawn(fun() ->
                lists:foreach(
                    fun(_) ->
                        try
                            Append(X, Payload)
                        catch
                            _ -> FailedAppendsIncr()
                        end
                    end,
                    lists:seq(1, 100)
                ),
                Countdown ! finished
            end)
        end,
        XS
    ),

    Report =
        spawn(fun Report() ->
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
            Report()
        end),

    receive
        finished ->
            exit(Report, finished)
    end,

    lists:foreach(
        fun(X) ->
            {Client, StreamName} = X,
            ok = hstreamdb_erlang:delete_stream(Client, StreamName, #{force => true}),
            ok = hstreamdb_erlang:stop_client_channel(Client)
        end,
        XS
    ).

bench() ->
    bench(#{
        producerNum => 100,
        payloadSize => 1,
        batchNum => 400,
        serverUrl => "http://127.0.0.1:6570",
        replicationFactor => 1,
        backlogDuration => 60 * 30,
        reportIntervalSeconds => 3
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
