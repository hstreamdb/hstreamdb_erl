%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(hstreamdb_batch_aggregator).

-include("hstreamdb.hrl").
-include("errors.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("kernel/include/logger.hrl").

-behaviour(gen_statem).

-export([
    append/2,
    flush/1,
    append_flush/2,
    append_flush/3,
    append_sync/2,
    append_sync/3
]).

-export([
    connect/1,
    ecpool_action/2,
    writer_name/1
]).

-export([
    on_stream_updated/3,
    on_init/1,
    on_terminate/4
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    code_change/4
]).

-type callback() ::
    {module(), atom(), [term()]} | {module(), atom()} | fun((term()) -> any()) | undefined.

-type options() :: #{
    stream := hstreamdb:stream(),
    callback := callback(),
    name := ecpool:pool_name(),
    interval => pos_integer(),
    batch_reap_timeout => pos_integer(),
    stream_invalidate_timeout => pos_integer(),
    max_records => pos_integer(),
    max_batches => pos_integer(),
    compression_type => none | gzip | zstd,
    discovery_backoff_opts => hstreamdb_backoff:options()
}.

-export_type([options/0, callback/0]).

-record(data, {
    name,
    writer_name,
    stream,
    compression_type,
    callback,
    stream_invalidate_timeout,

    queue,
    queue_size,
    queue_max_size,

    batch_tab,
    buffers,
    buffer_opts,

    discovery_backoff_opts,
    key_manager
}).

append(Producer, PKeyRecordOrRecords) ->
    RecordsByPK = records_by_pk(PKeyRecordOrRecords),
    do_while_ok(
        fun({PartitioningKey, Records}) ->
            append(Producer, PartitioningKey, Records)
        end,
        maps:to_list(RecordsByPK)
    ).

append(Producer, PartitioningKey, Records) ->
    ecpool:pick_and_do(
        {Producer, bin(PartitioningKey)},
        {?MODULE, ecpool_action, [{append, PartitioningKey, Records}]},
        no_handover
    ).

append_sync(Producer, {_PartitioningKey, _Record} = PKeyRecord) ->
    append_sync(Producer, PKeyRecord, infinity).

append_sync(Producer, {PartitioningKey, _Record} = PKeyRecord, Timeout) ->
    sync_request({Producer, bin(PartitioningKey)}, {append_sync, PKeyRecord}, Timeout).

flush(Producer) ->
    foreach_worker(
        fun(Pid) -> gen_server:call(Pid, flush) end,
        Producer
    ).

append_flush(Producer, {_PartitioningKey, _Record} = PKeyRecord) ->
    append_flush(Producer, PKeyRecord, infinity).

append_flush(Producer, {PartitioningKey, _Record} = PKeyRecord, Timeout) ->
    sync_request({Producer, bin(PartitioningKey)}, {append_flush, PKeyRecord}, Timeout).

%%-------------------------------------------------------------------------------------------------
%% Discovery
%%-------------------------------------------------------------------------------------------------

-define(DISCOVERY_KEY(Name), {?MODULE, Name}).

on_init(Name) -> cleanup(Name).
on_terminate(Name, _Vsn, _KeyManager, _ShardClientMgr) -> cleanup(Name).

on_stream_updated(_Name, {_OldVsn, KeyManager}, {_NewVsn, KeyManager}) ->
    ok;
on_stream_updated(Name, {_OldVsn, _OldKeyManager}, {NewVsn, NewKeyManager}) ->
    ?tp(hstreamdb_batch_aggregator_on_stream_updated, #{name => Name}),
    ok = set_key_manager(Name, NewVsn, NewKeyManager),
    foreach_worker(
        fun(Pid) -> gen_server:cast(Pid, stream_updated) end,
        Name
    ).

cleanup(Name) ->
    true = ets:match_delete(?DISCOVERY_TAB, {?DISCOVERY_KEY(Name), '_'}),
    ok.

key_manager(Name) ->
    case ets:lookup(?DISCOVERY_TAB, ?DISCOVERY_KEY(Name)) of
        [{_, {Vsn, KeyManager}}] ->
            {ok, Vsn, KeyManager};
        [] ->
            not_found
    end.

set_key_manager(Name, Vsn, KeyManager) ->
    true = ets:insert(?DISCOVERY_TAB, {?DISCOVERY_KEY(Name), {Vsn, KeyManager}}),
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal API: ecpool callbacks
%%-------------------------------------------------------------------------------------------------

-type pool_opts() :: list(any() | {opts, options()}).
-spec connect(pool_opts()) -> get_server:start_ret().
connect(PoolOptions) ->
    Options = proplists:get_value(opts, PoolOptions),
    gen_statem:start_link(?MODULE, [Options], []).

ecpool_action(Client, Req) ->
    gen_statem:call(Client, Req).

%%-------------------------------------------------------------------------------------------------
%% gen_statem callbacks
%%-------------------------------------------------------------------------------------------------

-define(discovering(Backoff), {discovering, Backoff}).
-define(active, active).
-define(invalidating, invalidating).
-define(terminating(Terminator), {terminating, Terminator}).

-define(next_state(STATE, DATA), begin
    ?tp(debug, hstreamdb_batch_aggregator_next_state, #{state => STATE}),
    {next_state, STATE, DATA}
end).
-define(next_state(STATE, DATA, ACTIONS), begin
    ?tp(debug, hstreamdb_batch_aggregator_next_state, #{state => STATE, actions => ACTIONS}),
    {next_state, STATE, DATA, ACTIONS}
end).

callback_mode() ->
    handle_event_function.

init([Options]) ->
    _ = process_flag(trap_exit, true),

    StreamName = maps:get(stream, Options),
    Name = maps:get(name, Options),

    Callback = maps:get(callback, Options, undefined),

    BatchTab = ets:new(?MODULE, [public]),

    BatchSize = maps:get(max_records, Options, ?DEFAULT_MAX_RECORDS),
    BatchMaxCount = maps:get(max_batches, Options, ?DEFAULT_MAX_BATCHES),

    BufferOpts = buffer_opts(Options),
    CompressionType = maps:get(compression_type, Options, ?DEFAULT_COMPRESSION),

    DiscoveryBackoffOpts = maps:get(
        discovery_backoff_opts, Options, ?DEFAULT_DISOVERY_BACKOFF_OPTIONS
    ),

    StreamInvalidateTimeout = maps:get(
        stream_invalidate_timeout, Options, ?DEFAULT_STREAM_INVALIDATE_TIMEOUT
    ),

    Data = #data{
        name = Name,
        writer_name = writer_name(Name),
        stream = StreamName,
        compression_type = CompressionType,
        callback = Callback,
        stream_invalidate_timeout = StreamInvalidateTimeout,

        batch_tab = BatchTab,
        buffer_opts = BufferOpts,
        buffers = #{},

        queue = [],
        queue_size = 0,
        queue_max_size = BatchSize * BatchMaxCount,

        discovery_backoff_opts = DiscoveryBackoffOpts,
        key_manager = undefined
    },
    Backoff = hstreamdb_backoff:new(DiscoveryBackoffOpts),
    {ok, ?discovering(Backoff), Data, [{state_timeout, 0, discover}]}.

%% Discovering

handle_event(state_timeout, discover, ?discovering(Backoff0), #data{name = Name} = Data0) ->
    case key_manager(Name) of
        {ok, _Vsn, KeyManager} ->
            Data1 = Data0#data{key_manager = KeyManager},
            Data2 = pour_queue_to_buffers(Data1),
            ?tp(batch_aggregator_discovered, #{name => Name}),
            ?next_state(?active, Data2);
        not_found ->
            {Delay, Backoff1} = hstreamdb_backoff:next_delay(Backoff0),
            ?next_state(?discovering(Backoff1), Data0, [{state_timeout, Delay, discover}])
    end;
handle_event(cast, stream_updated, ?discovering(_Backoff), _Data) ->
    {keep_state_and_data, [{state_timeout, 0, discover}]};
handle_event(cast, {stop, _Terminator}, ?discovering(_Backoff), _Data) ->
    {keep_state_and_data, postpone};
handle_event({call, From}, flush, ?discovering(_Backoff), _Data) ->
    {keep_state_and_data, [postpone, {reply, From, ok}]};
handle_event(
    {call, From}, {append, _PartitioningKey, _Records} = Req, ?discovering(_Backoff), Data
) ->
    enqueue_and_reply(Data, Req, From);
handle_event(
    {call, From},
    {sync_req, _Caller, _Req, _Timeout} = Req,
    ?discovering(_Backoff),
    Data
) ->
    enqueue_and_reply(Data, Req, From);
%% Terminating

handle_event({call, From}, _Req, ?terminating(_Terminator), _Data) ->
    {keep_state_and_data, [{reply, From, {error, ?ERROR_TERMINATING}}]};
handle_event(
    info,
    {write_result, #batch{shard_id = ShardId} = Batch, Result},
    ?terminating(_Terminator),
    Data
) ->
    {keep_state, handle_write_result(Data, ShardId, Batch, Result, false), [
        {next_event, internal, check_buffers_empty}
    ]};
handle_event(info, {shard_buffer_event, ShardId, Message}, ?terminating(_Terminator), Data) ->
    {keep_state, handle_shard_buffer_event(Data, ShardId, Message, false), [
        {next_event, internal, check_buffers_empty}
    ]};
handle_event(internal, check_buffers_empty, ?terminating(Terminator), Data) ->
    are_all_buffers_empty(Data) andalso erlang:send(Terminator, {empty, Terminator}),
    keep_state_and_data;
%% Invalidating

handle_event({call, From}, _Req, ?invalidating, _Data) ->
    {keep_state_and_data, [{reply, From, {error, ?ERROR_STREAM_CHANGED}}]};
handle_event(
    info,
    {write_result, #batch{shard_id = ShardId} = Batch, Result},
    ?invalidating,
    Data
) ->
    {keep_state, handle_write_result(Data, ShardId, Batch, Result, false), [
        {next_event, internal, check_buffers_empty}
    ]};
handle_event(info, {shard_buffer_event, ShardId, Message}, ?invalidating, Data) ->
    {keep_state, handle_shard_buffer_event(Data, ShardId, Message, false), [
        {next_event, internal, check_buffers_empty}
    ]};
handle_event(cast, stream_updated, ?invalidating, _Data) ->
    keep_state_and_data;
handle_event(cast, {stop, _Terminator}, ?invalidating, _Data) ->
    {keep_state_and_data, postpone};
handle_event(
    internal, check_buffers_empty, ?invalidating, #data{discovery_backoff_opts = BackoffOpts} = Data
) ->
    case are_all_buffers_empty(Data) of
        true ->
            Backoff = hstreamdb_backoff:new(BackoffOpts),
            ?next_state(?discovering(Backoff), Data, [{state_timeout, 0, discover}]);
        false ->
            ?next_state(?invalidating, Data)
    end;
handle_event(state_timeout, stream_invalidate_finish, ?invalidating, Data) ->
    Data1 = drop_inflight(Data, ?ERROR_STREAM_CHANGED),
    {keep_state, Data1, [{next_event, internal, check_buffers_empty}]};
%% Active

handle_event({call, From}, {append, _PartitioningKey, _Records} = Req, ?active, Data) ->
    {PartitioningKey, BufferRecords} = buffer_records(Req),
    {Resp, NData} = do_append(PartitioningKey, BufferRecords, false, Data),
    {keep_state, NData, [{reply, From, Resp}]};
handle_event({call, From}, flush, ?active, Data) ->
    {keep_state, flush_buffers(Data), [{reply, From, ok}]};
handle_event(
    {call, From}, {sync_req, _Caller, {append_sync, _}, _Timeout} = Req, ?active, Data
) ->
    {PartitioningKey, BufferRecords} = buffer_records(Req),
    {Resp, NData} = do_append(PartitioningKey, BufferRecords, false, Data),
    {keep_state, NData, [{reply, From, Resp}]};
handle_event(
    {call, From}, {sync_req, _Caller, {append_flush, _}, _Timeout} = Req, ?active, Data
) ->
    {PartitioningKey, BufferRecords} = buffer_records(Req),
    {Resp, NData} = do_append(PartitioningKey, BufferRecords, true, Data),
    {keep_state, NData, [{reply, From, Resp}]};
handle_event(cast, {stop, Terminator}, ?active, Data0) ->
    Data1 = flush_buffers(Data0),
    ?next_state(?terminating(Terminator), Data1, [{next_event, internal, check_buffers_empty}]);
handle_event(
    info,
    {write_result, #batch{shard_id = ShardId} = Batch, Result},
    ?active,
    Data
) ->
    {keep_state, handle_write_result(Data, ShardId, Batch, Result, true)};
handle_event(info, {shard_buffer_event, ShardId, Message}, ?active, Data) ->
    {keep_state, handle_shard_buffer_event(Data, ShardId, Message, true)};
handle_event(
    cast,
    stream_updated,
    ?active,
    #data{stream_invalidate_timeout = StreamInvalidateTimeout} = Data0
) ->
    Data1 = drop_buffers(Data0, ?ERROR_STREAM_CHANGED),
    ?next_state(?invalidating, Data1, [
        {next_event, internal, check_buffers_empty},
        {state_timeout, StreamInvalidateTimeout, stream_invalidate_finish}
    ]);
%% Fallbacks

handle_event({call, From}, Request, _State, _Data) ->
    {keep_state_and_data, [{reply, From, {error, {unknown_call, Request}}}]};
handle_event(info, _Request, _State, _Data) ->
    keep_state_and_data;
handle_event(cast, _Request, _State, _Data) ->
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

sync_request(PoolAndKey, Req, Timeout) ->
    Self = alias([reply]),
    case
        ecpool:pick_and_do(
            PoolAndKey,
            {?MODULE, ecpool_action, [{sync_req, Self, Req, Timeout}]},
            no_handover
        )
    of
        {error, _} = Error ->
            Error;
        ok ->
            wait_reply(Self, Timeout)
    end.

wait_reply(Self, Timeout) ->
    receive
        {reply, Self, Resp} ->
            Resp
    after Timeout ->
        unalias(Self),
        receive
            {reply, Self, Resp} ->
                Resp
        after 0 ->
            {error, timeout}
        end
    end.

foreach_worker(Fun, Pool) ->
    lists:foreach(
        fun({_Name, WPid}) ->
            ecpool_worker:exec(
                WPid,
                Fun,
                ?POOL_TIMEOUT
            )
        end,
        ecpool:workers(Pool)
    ).

append_to_queue(
    #data{queue = Queue, queue_size = Size, queue_max_size = MaxSize} = Data, Req
) ->
    Length = request_record_count(Req),
    case Size + Length > MaxSize of
        true ->
            {error, {queue_full, Req}};
        false ->
            BufferRecords = buffer_records(Req),
            {ok, Data#data{queue = [BufferRecords | Queue], queue_size = Size + Length}}
    end.

pour_queue_to_buffers(#data{queue = Queue} = Data) ->
    NData = lists:foldl(
        fun({PartitioningKey, BufferRecords}, Data0) ->
            %% We cannot overflow the buffers
            {ok, Data1} = do_append(PartitioningKey, BufferRecords, false, Data0),
            Data1
        end,
        Data,
        lists:reverse(Queue)
    ),
    NData#data{queue = [], queue_size = 0}.

get_shard_buffer(ShardId, #data{buffers = Buffers, buffer_opts = BufferOpts} = Data) ->
    case maps:get(ShardId, Buffers, undefined) of
        undefined ->
            Buffer = new_buffer(ShardId, BufferOpts, Data),
            NData = set_shard_buffer(ShardId, Buffer, Data),
            {Buffer, NData};
        Buffer ->
            {Buffer, Data}
    end.

set_shard_buffer(ShardId, Buffer, #data{buffers = Buffers} = Data) ->
    Data#data{buffers = maps:put(ShardId, Buffer, Buffers)}.

new_buffer(
    ShardId, BufferOpts, #data{batch_tab = BatchTab, callback = Callback, stream = Stream} = Data
) ->
    Opts = #{
        batch_tab => BatchTab,

        send_batch => fun(BufferBatch) ->
            write(ShardId, BufferBatch, Data)
        end,
        send_after => fun(Timeout, Message) ->
            erlang:send_after(Timeout, self(), {shard_buffer_event, ShardId, Message})
        end,
        cancel_send => fun(Ref) ->
            erlang:cancel_timer(Ref)
        end,
        send_reply => fun(From, Response) ->
            send_reply(From, Response, Callback, Stream)
        end
    },
    hstreamdb_buffer:new(maps:merge(BufferOpts, Opts)).

send_reply(From, Response, Callback, Stream) ->
    case From of
        undefined ->
            apply_callback(Callback, {{flush, Stream, 1}, Response});
        AliasRef when is_reference(AliasRef) ->
            erlang:send(AliasRef, {reply, AliasRef, Response});
        _ ->
            ?LOG_WARNING("[hstreamdb] producer_batch_aggregator: unexpected From: ~p", [From])
    end.

with_shard_buffer(
    PartitioningKey,
    Fun,
    Data0 = #data{
        key_manager = KeyManager
    }
) ->
    case hstreamdb_key_mgr:choose_shard(KeyManager, PartitioningKey) of
        {ok, ShardId} ->
            {Buffer, Data1} = get_shard_buffer(ShardId, Data0),
            case Fun(Buffer) of
                {ok, Buffer1} ->
                    Data2 = set_shard_buffer(ShardId, Buffer1, Data1),
                    {ok, Data2};
                {error, _} = Error ->
                    {Error, Data1}
            end;
        not_found ->
            {{error, shard_not_found}, Data0}
    end.

map_buffers(
    Data = #data{
        buffers = Buffers
    },
    Fun
) ->
    lists:foldl(
        fun(ShardId, Data0) ->
            {Buffer0, Data1} = get_shard_buffer(ShardId, Data0),
            Buffer1 = Fun(Buffer0),
            set_shard_buffer(ShardId, Buffer1, Data1)
        end,
        Data,
        maps:keys(Buffers)
    ).

do_append(PartitioningKey, BufferRecords, NeedFlush, Data) ->
    with_shard_buffer(
        PartitioningKey,
        fun(Buffer) ->
            maybe_flush(
                NeedFlush,
                hstreamdb_buffer:append(Buffer, BufferRecords)
            )
        end,
        Data
    ).

maybe_flush(true = _NeedFlush, {ok, Buffer}) ->
    {ok, hstreamdb_buffer:flush(Buffer)};
maybe_flush(_NeedFlush, Result) ->
    Result.

write(ShardId, #{batch_ref := Ref, tab := Tab}, #data{
    writer_name = WriterName, compression_type = CompressionType
}) ->
    ReqRef = make_ref(),
    Batch = #batch{
        batch_ref = Ref,
        shard_id = ShardId,
        tab = Tab,
        req_ref = ReqRef,
        compression_type = CompressionType
    },
    ok = ecpool:with_client(
        WriterName,
        fun(WriterPid) ->
            ok = hstreamdb_batch_writer:write(WriterPid, Batch)
        end
    ),
    ReqRef.

flush_buffers(Data) ->
    map_buffers(Data, fun(Buffer) -> hstreamdb_buffer:flush(Buffer) end).

drop_buffers(Data, Reason) ->
    map_buffers(Data, fun(Buffer) -> hstreamdb_buffer:drop(Buffer, Reason) end).

drop_inflight(Data, Reason) ->
    map_buffers(Data, fun(Buffer) -> hstreamdb_buffer:drop_inflight(Buffer, Reason) end).

handle_write_result(
    Data0,
    ShardId,
    #batch{req_ref = ReqRef},
    Result,
    MayRetry
) ->
    {Buffer0, Data1} = get_shard_buffer(ShardId, Data0),
    Buffer1 = hstreamdb_buffer:handle_batch_response(Buffer0, ReqRef, Result, MayRetry),
    set_shard_buffer(ShardId, Buffer1, Data1).

handle_shard_buffer_event(Data0, ShardId, Event, MayRetry) ->
    {Buffer0, Data1} = get_shard_buffer(ShardId, Data0),
    Buffer1 = hstreamdb_buffer:handle_event(Buffer0, Event, MayRetry),
    set_shard_buffer(ShardId, Buffer1, Data1).

are_all_buffers_empty(#data{buffers = Buffers}) ->
    lists:all(
        fun(Buffer) ->
            hstreamdb_buffer:is_empty(Buffer)
        end,
        maps:values(Buffers)
    ).

apply_callback({M, F}, R) ->
    erlang:apply(M, F, [R]);
apply_callback({M, F, A}, R) ->
    erlang:apply(M, F, [R | A]);
apply_callback(F, R) when is_function(F) ->
    F(R);
apply_callback(undefined, _) ->
    ok.

writer_name(ProducerName) ->
    {ProducerName, writer}.

records_by_pk(PKeyRecord) when is_tuple(PKeyRecord) ->
    records_by_pk([PKeyRecord]);
records_by_pk([]) ->
    #{};
records_by_pk([{PK, Record} | Rest]) ->
    maps:update_with(
        PK,
        fun(Records) ->
            [Record | Records]
        end,
        [Record],
        records_by_pk(Rest)
    ).

do_while_ok(_Fun, []) ->
    ok;
do_while_ok(Fun, [El | List]) ->
    case Fun(El) of
        ok ->
            do_while_ok(Fun, List);
        {ok, _} ->
            do_while_ok(Fun, List);
        {error, _} = Error ->
            Error
    end.

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(B) when is_binary(B) ->
    B.

buffer_opts(Options) ->
    #{
        flush_interval => maps:get(interval, Options, ?DEFAULT_INTERVAL),
        batch_timeout => maps:get(
            batch_reap_timeout, Options, ?DEFAULT_BATCH_REAP_TIMEOUT
        ),
        batch_size => maps:get(max_records, Options, ?DEFAULT_MAX_RECORDS),
        batch_max_count => maps:get(max_batches, Options, ?DEFAULT_MAX_BATCHES),
        max_retries => maps:get(batch_max_retries, Options, ?DEFAULT_BATCH_MAX_RETRIES),
        backoff_options => maps:get(batch_backoff_options, Options, ?DEFAULT_BATCH_BACKOFF_OPTIONS)
    }.

enqueue_and_reply(Data, Req, From) ->
    keep_state_and_reply(
        append_to_queue(Data, Req),
        From
    ).

request_record_count({sync_req, _Caller, _Req, _Timeout}) -> 1;
request_record_count({append, _PartitioningKey, Records}) -> length(Records).

buffer_records({append, PartitioningKey, Records}) ->
    BufferRecords = hstreamdb_buffer:to_buffer_records(Records),
    {PartitioningKey, BufferRecords};
buffer_records({sync_req, Caller, {_AppendKind, {PartitioningKey, Record}}, Timeout}) ->
    BufferRecords = hstreamdb_buffer:to_buffer_records([Record], Caller, Timeout),
    {PartitioningKey, BufferRecords}.

keep_state_and_reply({ok, Data}, From) ->
    {keep_state, Data, [{reply, From, ok}]};
keep_state_and_reply({error, _} = Error, From) ->
    {keep_state_and_data, [{reply, From, Error}]}.
