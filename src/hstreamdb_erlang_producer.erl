-module(hstreamdb_erlang_producer).

-behaviour(gen_statem).

-export([callback_mode/0]).
-export([start/0, start/1, start_link/0, start_link/1, init/1]).

-export([wait_for_append/3, appending/3]).

-export([readme/0]).

callback_mode() ->
    % Events are handled by one callback function per state.
    state_functions.

start_link() ->
    start_link([]).

start_link(Args) ->
    gen_statem:start_link(
        ?MODULE,
        Args,
        []
    ).

start() ->
    start([]).

start(Args) ->
    gen_statem:start(
        ?MODULE,
        Args,
        []
    ).

init(
    #{
        producerOptions := ProducerOptions,
        serverUrl := ServerUrl,
        streamName := StreamName
    } = Opts
) ->
    init(ProducerOptions, ServerUrl, StreamName).

init(ProducerOptions, ServerUrl, StreamName) ->
    SelfPid = self(),
    WorkerPid = spawn(
        fun() ->
            receive
                flush ->
                    gen_statem:call(
                        SelfPid, {append, #{record => [], flush => true}}
                    )
            end
        end
    ),
    timer:send_interval(5000, WorkerPid, flush),

    {ok, wait_for_append, #{
        recordBuffer =>
            #{
                batchInfo => #{
                    recordCount => 0,
                    bytes => 0
                },
                records => []
            },
        options => ProducerOptions,
        serverUrl => ServerUrl,
        streamName => StreamName
    }}.

%%--------------------------------------------------------------------

wait_for_append(
    {call, From},
    {_, EventContentMap} = EventContent,
    #{
        recordBuffer := RecordBuffer,
        options := ProducerOptions,
        serverUrl := ServerUrl,
        streamName := StreamName
    } = Data
) ->
    % logger:notice(#{
    %     msg => "producer: do wait_for_append",
    %     val => [EventContent, RecordBuffer]
    % }),

    Record = maps:get(record, EventContentMap),
    ProcessedBuffer = add_record_to_buffer(Record, RecordBuffer),
    ProcessedData = maps:update(
        recordBuffer, ProcessedBuffer, Data
    ),

    DoFlush = maps:get(doFlush, EventContentMap, false),

    gen_statem:reply(From, ok),
    case DoFlush orelse check_ready_for_append(ProcessedBuffer, ProducerOptions) of
        true ->
            Records = maps:get(records, ProcessedBuffer),
            io:format("~n~p~n", [exec_append(ServerUrl, Records, StreamName)]),

            {next_state, wait_for_append, maps:update(recordBuffer, empty_buffer(), Data)};
        false ->
            {next_state, wait_for_append, ProcessedData}
    end.

appending(
    {call, From},
    EventContent,
    #{
        recordBuffer := RecordBuffer,
        options := ProducerOptions
    } = Data
) ->
    % logger:notice(#{
    %     msg => "producer: do appending",
    %     val => EventContent
    % }),

    ProcessedBatchInfo = #{
        recordCount => 0,
        byte => 0
    },
    ProcessedBuffer = #{
        batchInfo => ProcessedBatchInfo,
        records => []
    },
    ProcessedData = maps:update(
        recordBuffer, ProcessedBuffer, Data
    ),

    gen_statem:reply(From, ok),
    {next_state, wait_for_append, ProcessedData}.

%%--------------------------------------------------------------------

check_ready_for_append(
    #{
        batchInfo := #{
            recordCount := RecordCount,
            bytes := Bytes
        }
    } = ProducerInfo,
    #{
        batchSetting := BatchSetting
    } = ProducerOptions
) ->
    % logger:notice(#{
    %     msg => "producer: check_ready_for_append",
    %     val => [ProducerInfo, ProducerOptions]
    % }),

    [
        RecordCountLimit,
        BytesLimit
    ] =
        BatchSettings = lists:map(
            fun(X) ->
                maps:get(X, BatchSetting, undefined)
            end,
            [
                recordCountLimit,
                bytesLimit
            ]
        ),
    true = lists:any(fun(X) -> X =/= undefined end, BatchSettings),

    lists:any(
        fun({X, XLimit}) ->
            case XLimit of
                undefined -> false;
                Limit when is_integer(Limit) -> X >= Limit
            end
        end,
        [
            {RecordCount, RecordCountLimit},
            {Bytes, BytesLimit}
        ]
    ).

add_record_to_buffer(
    Record,
    #{
        batchInfo := BatchInfo,
        records := _
    } = Buffer
) ->
    BatchInfo0 = maps:update_with(
        recordCount, fun(X) -> X + 1 end, BatchInfo
    ),
    BatchInfo1 = maps:update_with(
        bytes, fun(X) -> X + byte_size(Record) end, BatchInfo0
    ),
    Buffer0 = maps:update(
        batchInfo, BatchInfo1, Buffer
    ),
    Buffer1 = maps:update_with(
        records, fun(XS) -> [Record | XS] end, Buffer0
    ),

    % logger:notice(#{
    %     msg => "producer: do add_record_to_buffer",
    %     val => [Buffer, Buffer1]
    % }),

    Buffer1.

empty_buffer() ->
    #{
        batchInfo => #{
            recordCount => 0,
            bytes => 0
        },
        records => []
    }.

exec_append(
    ServerUrl, Records, StreamName
) ->
    {ok, Ch} = hstreamdb_erlang:start_client_channel(ServerUrl),
    OrderingKey = "",
    PayloadType = raw,
    Payload = Records,

    Key = rpc:async_call(node(), hstreamdb_erlang, append, [
        Ch, StreamName, OrderingKey, PayloadType, Payload
    ]),
    {ok, Ret} = rpc:yield(Key),
    hstreamdb_erlang:stop_client_channel(Ch),
    Ret.

%%--------------------------------------------------------------------

readme() ->
    {ok, Pid} = start_link(#{
        streamName => "xxxx",
        serverUrl => "http://127.0.0.1:6570",
        producerOptions => #{
            batchSetting => #{
                recordCountLimit => 3
            }
        }
    }),

    % {ok, Ch} = hstreamdb_erlang:start_client_channel("http://127.0.0.1:6570"),
    % hstreamdb_erlang:create_stream(Ch, "xxxx", 3, 15),

    gen_statem:call(
        Pid, {append, #{record => <<"00">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"01">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"02">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"03">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"04">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"05">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"06">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"07">>}}
    ),
    ok.