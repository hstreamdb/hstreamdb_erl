-module(hstreamdb_erlang_bench).

%% API exports
-export([main/1]).

-export([get_bytes/1, get_bytes/2]).
-export([remove_all_streams/1]).

-export([bench/0, bench/1]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    io:format("Args: ~p~n", [Args]),

    _ =
        case Args of
            ["readme"] ->
                readme();
            _ ->
                ok
        end,

    erlang:halt(0).

%%====================================================================
%% Internal functions
%%====================================================================

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).

readme() ->
    StreamName = string_format("test_stream-~p", [erlang:system_time(second)]),

    hstreamdb_erlang:start(normal, []),

    StartClientChannelRet = hstreamdb_erlang:start_client_channel("http://127.0.0.1:6570"),
    io:format("~p~n", [StartClientChannelRet]),
    {ok, Client} = StartClientChannelRet,

    io:format("~p~n", [hstreamdb_erlang:list_streams(Client)]),

    io:format("~p~n", [hstreamdb_erlang:create_stream(Client, StreamName, 3, 14)]),
    io:format("~p~n", [hstreamdb_erlang:list_streams(Client)]),

    XS = lists:seq(0, 100),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                hstreamdb_erlang:append(
                    Client,
                    StreamName,
                    "",
                    raw,
                    <<"this_is_a_binary_literal">>
                )
            ])
        end,
        XS
    ),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                hstreamdb_erlang:append(
                    Client,
                    StreamName,
                    "",
                    raw,
                    lists:duplicate(10, <<"this_is_a_binary_literal">>)
                )
            ])
        end,
        XS
    ),

    io:format("~p~n", [hstreamdb_erlang:delete_stream(Client, StreamName)]),
    io:format("~p~n", [hstreamdb_erlang:list_streams(Client)]),

    io:format("~p~n", [hstreamdb_erlang:stop_client_channel(Client)]),

    ok.

bit_size_128() -> (<<"___hstream.io___">>).

get_bytes(Size) -> get_bytes(Size, k).

get_bytes(Size, Unit) ->
    SizeBytes =
        case Unit of
            k -> Size * 1024;
            m -> Size * 1024 * 1024
        end,
    lists:foldl(
        fun(X, Acc) -> <<X/binary, Acc/binary>> end,
        <<"">>,
        lists:duplicate(round(SizeBytes / 128), bit_size_128())
    ).

remove_all_streams(Client) ->
    {ok, Streams} = hstreamdb_erlang:list_streams(Client),
    lists:foreach(
        fun(Stream) ->
            {ok} = hstreamdb_erlang:delete_stream(Client, Stream, #{force => true})
        end,
        lists:map(
            fun(Stream) -> maps:get(streamName, Stream) end, Streams
        )
    ).

bench(Opts) ->
    ProducerNum = maps:get(producerNum, Opts),
    PayloadSize = maps:get(payloadSize, Opts),
    ServerUrl = maps:get(serverUrl, Opts),
    ReplicationFactor = maps:get(replicationFactor, Opts),
    BacklogDuration = maps:get(backlogDuration, Opts),
    ReportIntervalSeconds = maps:get(reportIntervalSeconds, Opts),

    hstreamdb_erlang:start(normal, []),

    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    remove_all_streams(Channel),

    XS =
        lists:map(
            fun(X) ->
                StreamName = string_format("test_stream-~p-~p", [X, erlang:system_time(second)]),
                {ok, Client} = hstreamdb_erlang:start_client_channel(ServerUrl),
                {ok} = hstreamdb_erlang:create_stream(
                    Client, StreamName, ReplicationFactor, BacklogDuration
                ),
                {
                    Client,
                    StreamName
                }
            end,
            lists:seq(0, ProducerNum)
        ),

    SuccessAppends = atomics:new(1, [{signed, false}]),
    FailedAppends = atomics:new(1, [{signed, false}]),

    SuccessAppendsIncr = fun() -> atomics:add(SuccessAppends, 1, 1) end,
    FailedAppendsIncr = fun() -> atomics:add(FailedAppends, 1, 1) end,
    % SuccessAppendsReset = fun() -> atomics:put(SuccessAppends, 1, 0) end,
    % FailedAppendsReset = fun() -> atomics:put(FailedAppends, 1, 0) end,
    SuccessAppendsGet = fun() -> atomics:get(SuccessAppends, 1) end,
    FailedAppendsGet = fun() -> atomics:get(FailedAppends, 1) end,

    LastSuccessAppends = atomics:new(1, [{signed, false}]),
    LastFailedAppends = atomics:new(1, [{signed, false}]),

    LastSuccessAppendsPut = fun(X) -> atomics:put(LastSuccessAppends, 1, X) end,
    LastFailedAppendsPut = fun(X) -> atomics:put(LastFailedAppends, 1, X) end,
    % LastSuccessAppendsReset = fun() -> atomics:put(LastSuccessAppends, 1, 0) end,
    % LastFailedAppendsReset = fun() -> atomics:put(LastFailedAppends, 1, 0) end,
    LastSuccessAppendsGet = fun() -> atomics:get(LastSuccessAppends, 1) end,
    LastFailedAppendsGet = fun() -> atomics:get(LastFailedAppends, 1) end,

    Append = fun({Client, StreamName}, X) ->
        case hstreamdb_erlang:append(Client, StreamName, "", raw, X) of
            {ok, _} -> SuccessAppendsIncr();
            {err, _} -> FailedAppendsIncr()
        end
    end,

    Payload = get_bytes(PayloadSize),

    Pid = self(),
    Countdown = spawn(fun() -> countdown(length(XS), Pid) end),

    lists:foreach(
        fun(X) ->
            spawn(
                fun() ->
                    lists:foreach(
                        fun(_) ->
                            try
                                Append(X, Payload)
                            catch
                                _ -> FailedAppendsIncr()
                            end
                        end,
                        lists:seq(0, 100)
                    ),
                    Countdown ! finished
                end
            )
        end,
        XS
    ),

    Report = spawn(
        fun Report() ->
            LastSuccessAppendsPut(SuccessAppendsGet()),
            LastFailedAppendsPut(FailedAppendsGet()),

            timer:sleep(ReportIntervalSeconds * 1000),

            io:format("[BENCH]: SuccessAppends=~p, FailedAppends=~p~n", [
                (SuccessAppendsGet() - LastSuccessAppendsGet()) / ReportIntervalSeconds,
                (FailedAppendsGet() - LastFailedAppendsGet()) / ReportIntervalSeconds
            ]),
            Report()
        end
    ),

    receive
        finished ->
            % io:format("[BENCH]: finished"),
            exit(Report, finished)
    end,

    lists:foreach(
        fun(X) ->
            {Client, StreamName} = X,
            {ok} = hstreamdb_erlang:delete_stream(Client, StreamName, #{force => true}),
            ok = hstreamdb_erlang:stop_client_channel(Client)
        end,
        XS
    ).

bench() ->
    bench(
        #{
            producerNum => 100,
            payloadSize => 1,
            serverUrl => "http://127.0.0.1:6570",
            replicationFactor => 1,
            backlogDuration => 60 * 30,
            reportIntervalSeconds => 3
        }
    ).

countdown(N, Pid) ->
    case N of
        0 ->
            % logger:notice("countdown: finished"),
            Pid ! finished;
        _ ->
            receive
                finished ->
                    % logger:notice("countdown: receive finished, left ~p", [N - 1]),
                    countdown(N - 1, Pid)
            end
    end.
