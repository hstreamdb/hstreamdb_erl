-module(hstreamdb_erlang_producer).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([start/1, start_link/1, append/2, flush/1]).
-export([
    build_start_args/1,
    build_batch_setting/1, build_batch_setting/3,
    build_producer_option/4,
    build_record/1, build_record/2, build_record/3
]).

% --------------------------------------------------------------------------------

build_start_args(ProducerOption) ->
    #{
        producer_option => ProducerOption
    }.

init(
    #{
        producer_option := ProducerOption
    } = _Args
) ->
    #{
        server_url := ServerUrl,
        stream_name := StreamName,
        batch_setting := BatchSetting,
        return_pid := ReturnPid
    } = ProducerOption,

    {ok, _, AppendWorkerPoolName} = start_append_worker_pool(ServerUrl, StreamName, ReturnPid),

    ProducerStatus = neutral_producer_status(),

    #{age_limit := AgeLimit} = BatchSetting,

    AgeLimitWorkerPid =
        case AgeLimit of
            undefined ->
                undefined;
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
                    {t_ref, TRef} ->
                        TRef
                end
        end,

    ProducerResource = build_producer_resource(AgeLimitWorkerPid, AppendWorkerPoolName),

    State = build_producer_state(
        ProducerStatus, ProducerOption, ProducerResource
    ),
    {ok, State}.

% --------------------------------------------------------------------------------

start(Args) ->
    gen_server:start(?MODULE, Args, []).

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

append(Producer, Record) ->
    gen_server:call(
        Producer,
        build_append_request(
            Record
        )
    ).

flush(Producer) ->
    gen_server:call(
        Producer,
        build_flush_request()
    ).

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
    true = lists:any(fun(X) -> X /= undefined end, BatchSettingList).

build_batch_setting(RecordCountLimit, BytesLimit, AgeLimit) ->
    check_batch_setting(RecordCountLimit, BytesLimit, AgeLimit),
    #{
        record_count_limit => RecordCountLimit,
        bytes_limit => BytesLimit,
        age_limit => AgeLimit
    }.

build_producer_option(ServerUrl, StreamName, BatchSetting, ReturnPid) ->
    #{
        server_url => ServerUrl,
        stream_name => StreamName,
        batch_setting => BatchSetting,
        return_pid => ReturnPid
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

build_producer_resource(AgeLimitWorker, AppendWorkerPool) ->
    #{
        age_limit_worker => AgeLimitWorker,
        append_worker_pool => AppendWorkerPool
    }.

build_producer_state(ProducerStatus, ProducerOption, ProducerResource) ->
    #{
        producer_status => ProducerStatus,
        producer_option => ProducerOption,
        producer_resource => ProducerResource
    }.

neutral_batch_status() ->
    RecordCount = 0,
    Bytes = 0,
    build_batch_status(RecordCount, Bytes).

neutral_producer_status() ->
    Records = #{},
    BatchStatus = neutral_batch_status(),
    build_producer_status(Records, BatchStatus).

% --------------------------------------------------------------------------------

add_to_buffer(
    {PayloadType, Payload, OrderingKey} = _Record,
    #{
        producer_status := ProducerStatus,
        producer_option := ProducerOption,
        producer_resource := ProducerResource
    } = _State
) when is_binary(Payload) ->
    #{
        records := Records,
        batch_status := BatchStatus
    } = ProducerStatus,
    #{
        record_count := RecordCount,
        bytes := Bytes
    } = BatchStatus,

    X = {PayloadType, Payload},
    NewRecords =
        case maps:is_key(OrderingKey, Records) of
            false ->
                maps:put(OrderingKey, [X], Records);
            true ->
                maps:update_with(
                    OrderingKey,
                    fun(XS) -> [X | XS] end,
                    Records
                )
        end,

    NewRecordCount = RecordCount + 1,
    NewBytes = Bytes + byte_size(Payload),
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

handle_call({Method, Body} = _Request, _From, State) ->
    case Method of
        flush ->
            exec_flush(Body, State);
        append ->
            exec_append(Body, State)
    end.

handle_cast(_, _) ->
    throw(hstreamdb_exception).

handle_info(Info, State) ->
    case Info of
        flush ->
            {_, FlushRequest} = build_flush_request(),
            {reply, {ok, _}, NewState} = exec_flush(FlushRequest, State),
            {noreply, NewState};
        _ ->
            NewState = State,
            {noreply, NewState}
    end.

% --------------------------------------------------------------------------------

build_record(Payload) when is_binary(Payload) ->
    build_record(raw, Payload, "").

build_record(PayloadType, Payload) when is_atom(PayloadType) andalso is_binary(Payload) ->
    build_record(PayloadType, Payload, "");
build_record(Payload, OrderingKey) when is_binary(Payload) andalso is_list(OrderingKey) ->
    build_record(raw, Payload, OrderingKey).

build_record(PayloadType, Payload, OrderingKey) ->
    {PayloadType, Payload, OrderingKey}.

build_append_request(Record) ->
    {append, #{
        record => Record
    }}.

build_flush_request() ->
    {flush, #{}}.

% --------------------------------------------------------------------------------

do_append(Records, AppendWorkerPool) ->
    Fun = fun(OrderingKey, Payloads) ->
        do_append_for_key(OrderingKey, Payloads, AppendWorkerPool)
    end,
    maps:foreach(Fun, Records).

do_append_for_key(OrderingKey, Payloads, AppendWorkerPool) ->
    AppendRequest = hstreamdb_erlang_append_worker:build_append_request(OrderingKey, Payloads),
    wpool:cast(AppendWorkerPool, AppendRequest).

exec_flush(
    #{} = _FlushRequest,
    #{
        producer_status := ProducerStatus,
        producer_resource := ProducerResource
    } = State
) ->
    #{
        records := Records
    } = ProducerStatus,
    #{
        append_worker_pool := AppendWorkerPool
    } = ProducerResource,

    Reply = do_append(Records, AppendWorkerPool),

    NewState = clear_buffer(State),
    {reply, Reply, NewState}.

exec_append(
    #{
        record := Record
    } = _AppendRequest,
    State
) ->
    State0 = add_to_buffer(Record, State),
    Reply = ok,
    case check_buffer_limit(State0) of
        true ->
            {_, FlushRequest} = build_flush_request(),
            {reply, _, NewState} = exec_flush(FlushRequest, State0),
            {reply, Reply, NewState};
        false ->
            NewState = State0,
            {reply, Reply, NewState}
    end.

% --------------------------------------------------------------------------------

start_append_worker_pool(ServerUrl, StreamName, ReturnPid) ->
    Name = list_to_atom(
        hstreamdb_erlang_utils:string_format("~s-~s", [
            "HSTREAM_WORKER", hstreamdb_erlang_utils:uid()
        ])
    ),
    {ok, Pid} = wpool:start_pool(
        Name,
        [
            {worker_type, gen_server},
            {worker,
                {hstreamdb_erlang_append_worker,
                    hstreamdb_erlang_append_worker:build_start_args(
                        hstreamdb_erlang_append_worker:build_appender_option(
                            ServerUrl, StreamName, ReturnPid
                        )
                    )}}
        ]
    ),
    {ok, Pid, Name}.
