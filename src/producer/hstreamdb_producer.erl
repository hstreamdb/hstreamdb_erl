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

-module(hstreamdb_producer).

-include("hstreamdb.hrl").

-behaviour(gen_server).

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
    writer_name/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([options/0]).

-record(state, {
    name,
    writer_name,
    stream,
    compression_type,
    callback,
    batch_tab,
    buffers,
    buffer_opts,
    key_manager,

    terminator = undefined
}).

append(Producer, {_PartitioningKey, _Record} = PKeyRecord) ->
    ecpool:with_client(
        Producer,
        fun(Pid) ->
            gen_server:call(Pid, {append, PKeyRecord})
        end
    ).

append_sync(Producer, {_PartitioningKey, _Record} = PKeyRecord) ->
    append_sync(Producer, PKeyRecord, infinity).

append_sync(Producer, {_PartitioningKey, _Record} = PKeyRecord, Timeout) ->
    sync_request(Producer, {append_sync, PKeyRecord}, Timeout).

flush(Producer) ->
    foreach_worker(
        fun(Pid) -> gen_server:call(Pid, flush) end,
        Producer
    ).

append_flush(Producer, {_PartitioningKey, _Record} = PKeyRecord) ->
    append_flush(Producer, PKeyRecord, infinity).

append_flush(Producer, {_PartitioningKey, _Record} = PKeyRecord, Timeout) ->
    sync_request(Producer, {append_flush, PKeyRecord}, Timeout).

%%-------------------------------------------------------------------------------------------------
%% ecpool part

-type callback() :: {module(), atom(), [term()]} | {module(), atom()} | fun((term()) -> any()) | undefined.

-type options() :: #{
    stream := hstreamdb:stream(),
    callback := callback(),
    mgr_client_options := hstreamdb_client:options(),
    producer_name := ecpool:pool_name(),
    interval => pos_integer(),
    batch_reap_timeout => pos_integer(),
    max_records => pos_integer(),
    max_batches => pos_integer(),
    compression_type => none | gzip | zstd
}.

-spec connect(options()) -> get_server:start_ret().
connect(PoolOptions) ->
    Options = proplists:get_value(opts, PoolOptions),
    gen_server:start_link(?MODULE, [Options], []).

%% -------------------------------------------------------------------------------------------------
%% gen_server part

init([Options]) ->
    _ = process_flag(trap_exit, true),

    StreamName = maps:get(stream, Options),
    MgrClientOptions = maps:get(mgr_client_options, Options),
    ProducerName = maps:get(producer_name, Options),

    Callback = maps:get(callback, Options, undefined),

    BatchTab = ets:new(?MODULE, [public]),

    BufferOpts = #{
        flush_interval => maps:get(interval, Options, ?DEFAULT_INTERVAL),
        batch_timeout => maps:get(
            batch_reap_timeout, Options, ?DEFAULT_BATCH_REAP_TIMEOUT
        ),
        batch_size => maps:get(max_records, Options, ?DEFAULT_MAX_RECORDS),
        batch_max_count => maps:get(max_batches, Options, ?DEFAULT_MAX_BATCHES)
    },
    CompressionType = maps:get(compression_type, Options, ?DEFAULT_COMPRESSION),

    case hstreamdb_client:start(MgrClientOptions) of
        {ok, Client} ->
            {ok, #state{
                name = ProducerName,
                writer_name = writer_name(ProducerName),
                stream = StreamName,
                compression_type = CompressionType,
                callback = Callback,
                batch_tab = BatchTab,
                buffer_opts = BufferOpts,
                buffers = #{},
                key_manager = hstreamdb_key_mgr:start(Client, StreamName)
            }};
        {error, _} = Error ->
            Error
    end.

handle_call(_Req, _From, #state{terminator = Terminator} = State) when Terminator =/= undefined ->
    {reply, {error, terminating}, State};
handle_call({append, PKeyRecord}, _From, State) ->
    {Resp, NState} = with_shart_buffer(
        PKeyRecord,
        fun(Buffer, Record) -> do_append(Buffer, Record) end,
        State
    ),
    {reply, Resp, NState};
handle_call(flush, _From, State) ->
    {reply, ok, do_flush(State)};
handle_call({sync_req, From, {append_flush, PKeyRecord}, Timeout}, _From, State) ->
    {Resp, NState} = with_shart_buffer(
        PKeyRecord,
        fun(Buffer, Record) -> do_append_flush(Buffer, Record, From, Timeout) end,
        State
    ),
    {reply, Resp, NState};
handle_call({sync_req, From, {append_sync, PKeyRecord}, Timeout}, _From, State) ->
    {Resp, NState} = with_shart_buffer(
        PKeyRecord,
        fun(Buffer, Record) -> do_append_sync(Buffer, Record, From, Timeout) end,
        State
    ),
    {reply, Resp, NState};
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_call, Request}}, State}.

handle_info(
    {write_result, ShardId, BatchRef, Result},
    State
) ->
    {noreply, maybe_report_empty(handle_write_result(State, ShardId, BatchRef, Result))};
handle_info({shard_buffer_event, ShardId, Message}, State) ->
    {noreply, maybe_report_empty(handle_shard_buffer_event(State, ShardId, Message))};
handle_info(_Request, State) ->
    {noreply, State}.

handle_cast({stop, Terminator}, State0) ->
    State1 = do_flush(State0),
    {noreply, maybe_report_empty(State1#state{terminator = Terminator})};
handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

sync_request(Producer, Req, Timeout) ->
    Self = alias([reply]),
    case
        ecpool:with_client(
            Producer,
            fun(Pid) ->
                gen_server:call(Pid, {sync_req, Self, Req, Timeout})
            end
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

get_shard_buffer(ShardId, #state{buffers = Buffers, buffer_opts = BufferOpts} = St) ->
    case maps:get(ShardId, Buffers, undefined) of
        undefined ->
            Buffer = new_buffer(ShardId, BufferOpts, St),
            NSt = set_shard_buffer(ShardId, Buffer, St),
            {Buffer, NSt};
        Buffer ->
            {Buffer, St}
    end.

set_shard_buffer(ShardId, Buffer, #state{buffers = Buffers} = St) ->
    St#state{buffers = maps:put(ShardId, Buffer, Buffers)}.

new_buffer(
    ShardId, BufferOpts, #state{batch_tab = BatchTab, callback = Callback, stream = Stream} = St
) ->
    Opts = #{
        batch_tab => BatchTab,

        send_batch => fun(BufferBatch) ->
            write(ShardId, BufferBatch, St)
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
            logger:warning("[hstreamdb_producer] Unexpected From: ~p", [From])
    end.

with_shart_buffer(
    {PartitioningKey, Record},
    Fun,
    State0 = #state{
        key_manager = KeyManager0
    }
) ->
    {ShardId, KeyManager1} = hstreamdb_key_mgr:choose_shard(KeyManager0, PartitioningKey),
    {Buffer, State1} = get_shard_buffer(ShardId, State0),
    case Fun(Buffer, Record) of
        {ok, Buffer1} ->
            State2 = set_shard_buffer(ShardId, Buffer1, State1),
            {ok, State2#state{key_manager = KeyManager1}};
        {error, _} = Error ->
            {Error, State1#state{key_manager = KeyManager1}}
    end.

do_append(Buffer, Record) ->
    hstreamdb_buffer:append(Buffer, undefined, [Record], infinity).

do_append_flush(Buffer0, Record, From, Timeout) ->
    case hstreamdb_buffer:append(Buffer0, From, [Record], Timeout) of
        {ok, Buffer1} ->
            {ok, hstreamdb_buffer:flush(Buffer1)};
        {error, _} = Error ->
            Error
    end.

do_append_sync(Buffer, Record, From, Timeout) ->
    hstreamdb_buffer:append(Buffer, From, [Record], Timeout).

write(ShardId, #{batch_ref := Ref, tab := Tab}, #state{
    writer_name = WriterName, compression_type = CompressionType
}) ->
    Batch = #batch{
        id = Ref,
        shard_id = ShardId,
        tab = Tab,
        compression_type = CompressionType
    },
    ecpool:with_client(
        WriterName,
        fun(WriterPid) ->
            ok = hstreamdb_batch_writer:write(WriterPid, Batch)
        end
    ).

do_flush(
    State = #state{
        buffers = Buffers
    }
) ->
    lists:foldl(
        fun(ShardId, St0) ->
            {Buffer0, St1} = get_shard_buffer(ShardId, St0),
            Buffer1 = hstreamdb_buffer:flush(Buffer0),
            set_shard_buffer(ShardId, Buffer1, St1)
        end,
        State,
        maps:keys(Buffers)
    ).

handle_write_result(
    State0,
    ShardId,
    BatchRef,
    Result
) ->
    {Buffer0, State1} = get_shard_buffer(ShardId, State0),
    Buffer1 = hstreamdb_buffer:handle_batch_response(Buffer0, BatchRef, Result),
    set_shard_buffer(ShardId, Buffer1, State1).

handle_shard_buffer_event(State0, ShardId, Event) ->
    {Buffer0, State1} = get_shard_buffer(ShardId, State0),
    Buffer1 = hstreamdb_buffer:handle_event(Buffer0, Event),
    set_shard_buffer(ShardId, Buffer1, State1).

maybe_report_empty(#state{terminator = undefined} = State) ->
    State;
maybe_report_empty(#state{terminator = Terminator} = State) ->
    case are_all_buffers_empty(State) of
        true ->
            erlang:send(Terminator, {empty, Terminator}),
            State;
        false ->
            State
    end.

are_all_buffers_empty(#state{buffers = Buffers}) ->
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
