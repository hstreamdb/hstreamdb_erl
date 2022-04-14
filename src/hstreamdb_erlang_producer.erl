-module(hstreamdb_erlang_producer).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([start/1, start_link/1]).

-export([readme/0]).

% --------------------------------------------------------------------------------

init(
    #{
        producer_option := ProducerOption
    } = _Args
) ->
    ProducerStatus = neutral_producer_status(),

    #{age_limit := AgeLimit} = ProducerOption,

    ProducerResource =
        case AgeLimit of
            undefined ->
                AgeLimitWorker = undefined,
                build_producer_resource(AgeLimitWorker);
            _ ->
                SelfPid = self(),
                _ = spawn(
                    fun() ->
                        {ok, TRef} =
                            timer:send_interval(AgeLimit, SelfPid, flush),
                        SelfPid ! {t_ref, TRef}
                    end
                ),

                receive
                    {t_ref, AgeLimitWorker} ->
                        Ret = build_producer_resource(AgeLimitWorker)
                end,
                Ret
        end,

    State = build_producer_state(
        ProducerStatus, ProducerOption, ProducerResource
    ),
    {ok, State}.

% --------------------------------------------------------------------------------

start(
    #{
        producer_option := _ProducerOption
    } = Args
) ->
    gen_server:start(?MODULE, Args, []).

start_link(
    #{
        producer_option := _ProducerOption
    } = Args
) ->
    gen_server:start_link(?MODULE, Args, []).

% --------------------------------------------------------------------------------

build_batch_setting(BatchSetting) when is_tuple(BatchSetting) ->
    {BatchSettingAtom, BatchSettingValue} = BatchSetting,
    BatchSettingMap = #{BatchSettingAtom => BatchSettingValue},
    build_batch_setting(BatchSettingMap);
build_batch_setting(BatchSetting) when is_list(BatchSetting) ->
    BatchSettingMap = maps:from_list(BatchSetting),
    build_batch_setting(BatchSettingMap);
build_batch_setting(BatchSetting) when is_map(BatchSetting) ->
    Get = fun(X) -> maps:get(X, BatchSetting, undefined) end,
    RecordCountLimit = Get(record_count_limit),
    BytesLimit = Get(bytes_limit),
    AgeLimit = Get(age_limit),
    build_batch_setting(RecordCountLimit, BytesLimit, AgeLimit).

check_batch_setting(RecordCountLimit, BytesLimit, AgeLimit) ->
    BatchSettingList = [RecordCountLimit, BytesLimit, AgeLimit],
    try
        true = lists:any(fun(X) -> X /= undefined end, BatchSettingList)
    catch
        error:_ ->
            throw(hstreamdb_exception)
    end.

build_batch_setting(RecordCountLimit, BytesLimit, AgeLimit) ->
    check_batch_setting(RecordCountLimit, BytesLimit, AgeLimit),
    #{
        record_count_limit => RecordCountLimit,
        bytes_limit => BytesLimit,
        age_limit => AgeLimit
    }.

build_producer_option(ServerUrl, StreamName, BatchSetting) ->
    #{
        server_url => ServerUrl,
        stream_name => StreamName,
        batch_setting => BatchSetting
    }.

build_batch_status(RecordCount, Bytes) ->
    #{
        record_count => RecordCount,
        bytes => Bytes
    }.

build_producer_status(Records, BatchStatus) ->
    #{
        records => Records,
        batch_status => BatchStatus
    }.

build_producer_resource(AgeLimitWorker) ->
    #{
        age_limit_worker => AgeLimitWorker
    }.

build_producer_state(ProducerStatus, ProducerOption, ProducerResource) ->
    #{
        producer_status => ProducerStatus,
        producer_option => ProducerOption,
        producer_resource => ProducerResource
    }.

neutral_batch_status() ->
    build_batch_status(0, 0).

neutral_producer_status() ->
    build_producer_status([], neutral_batch_status()).

% --------------------------------------------------------------------------------

add_to_buffer(
    Record,
    #{
        producer_status := ProducerStatus,
        producer_option := ProducerOption,
        producer_resource := ProducerResource
    } = _State
) when is_binary(Record) ->
    #{
        records := Records,
        batch_status := BatchStatus
    } = ProducerStatus,
    #{
        record_count := RecordCount,
        bytes := Bytes
    } = BatchStatus,

    NewRecords = [Record | Records],
    NewRecordCount = RecordCount + 1,
    NewBytes = Bytes + byte_size(Record),
    NewBatchStatus = build_batch_status(NewRecordCount, NewBytes),

    NewProducerStatus = build_producer_status(NewRecords, NewBatchStatus),
    build_producer_state(
        NewProducerStatus, ProducerOption, ProducerResource
    ).

check_buffer_limit(
    #{
        producer_status := ProducerStatus,
        producer_option := ProducerOption
    } = _State
) ->
    #{
        batch_status := #{
            record_count := RecordCount,
            bytes := Bytes
        }
    } = ProducerStatus,
    #{
        batch_setting := #{
            record_count_limit := RecordCountLimit,
            bytes_limit := BytesLimit
        }
    } = ProducerOption,

    CheckLimit = fun({X, XS}) ->
        case XS of
            undefined -> false;
            XLimit when is_integer(XLimit) -> X >= XLimit
        end
    end,

    CheckLimit({RecordCount, RecordCountLimit}) orelse
        CheckLimit({Bytes, BytesLimit}).

clear_buffer(
    #{
        producer_option := ProducerOption,
        producer_resource := ProducerResource
    } = _State
) ->
    ProducerStatus = neutral_producer_status(),
    build_producer_state(
        ProducerStatus, ProducerOption, ProducerResource
    ).

% --------------------------------------------------------------------------------

handle_call({Method, Body} = Request, From, State) ->
    case Method of
        flush -> exec_flush(Body, State);
        append -> exec_append(Body, State);
        _ -> throw(hstreamdb_exception)
    end.

handle_cast(_, _) ->
    throw(hstreamdb_exception).

handle_info(Info, State) ->
    case Info of
        flush ->
            FlushRequest = build_flush_request(),
            {reply, {ok, _}, NewState} = exec_flush(FlushRequest, State),
            {noreply, NewState};
        _ ->
            throw(hstreamdb_exception)
    end.

% --------------------------------------------------------------------------------

build_append_request(Record) ->
    {append, #{
        record => Record
    }}.

build_flush_request() ->
    {flush, #{}}.

% --------------------------------------------------------------------------------

append(Records, ServerUrl, StreamName) ->
    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    OrderingKey = "",
    PayloadType = raw,
    Ret =
        hstreamdb_erlang:append(
            Channel, StreamName, OrderingKey, PayloadType, Records
        ),
    _ = hstreamdb_erlang:stop_client_channel(Channel),
    Ret.

exec_flush(
    _FlushRequest,
    #{
        producer_status := ProducerStatus,
        producer_option := ProducerOption
    } = State
) ->
    #{
        records := Records
    } = ProducerStatus,
    #{
        server_url := ServerUrl,
        stream_name := StreamName
    } = ProducerOption,

    Reply = append(Records, ServerUrl, StreamName),

    NewState = clear_buffer(State),
    {reply, Reply, NewState}.

exec_append(
    #{
        record := Record
    } = _AppendRequest,
    State
) ->
    State0 = add_to_buffer(Record, State),
    case check_buffer_limit(State0) of
        true ->
            FlushRequest = build_flush_request(),
            exec_flush(FlushRequest, State0);
        false ->
            Reply = ok,
            NewState = State0,
            {reply, Reply, NewState}
    end.

% --------------------------------------------------------------------------------

readme() ->
    ServerUrl = "http://127.0.0.1:6570",
    StreamName = "___v2_test___",
    BatchSetting = build_batch_setting({record_count_limit, 3}),

    % {ok, _} = hstreamdb_erlang:start(normal, []),
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
        producer_option => build_producer_option(ServerUrl, StreamName, BatchSetting)
    },
    {ok, ProducerPid} = start_link(StartArgs),

    io:format("StartArgs: ~p~n", [StartArgs]),
    io:format("~p~n", [
        gen_server:call(
            ProducerPid,
            build_append_request(<<"00">>)
        )
    ]),
    io:format("~p~n", [
        gen_server:call(
            ProducerPid,
            build_append_request(<<"01">>)
        )
    ]),
    io:format("~p~n", [
        gen_server:call(
            ProducerPid,
            build_append_request(<<"02">>)
        )
    ]),
    io:format("~p~n", [
        gen_server:call(
            ProducerPid,
            build_append_request(<<"03">>)
        )
    ]),
    io:format("~p~n", [
        gen_server:call(
            ProducerPid,
            build_append_request(<<"04">>)
        )
    ]),

    ok.
