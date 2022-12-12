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

-define(DEFAULT_MAX_RECORDS, 100).
-define(DEFAULT_MAX_BATCHES, 500).
-define(DEFAULT_INTERVAL, 3000).
-define(POOL_TIMEOUT, 60000).
-define(DEFAULT_WRITER_POOL_SIZE, 64).
-define(DEFAULT_BATCH_REAP_TIMEOUT, 120000).

-behaviour(gen_server).

-export([ start/2
        , stop/1
        , append/2
        , flush/1
        , append_flush/2
        , connect/1
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
    name,
    writer_name,
    stream,
    compression_type,
    callback,
    max_records,
    max_batches,
    interval,
    batch_reap_timeout,
    record_map,
    flush_deadline_map,
    key_manager,
    batches,
    current_batches,
    timer_ref
}).


start(Producer, Options) ->
    case ecpool:start_sup_pool(Producer, ?MODULE, [{producer_name, Producer} | Options]) of
        {ok, _Pid} ->
            WriterPoolSise = proplists:get_value(writer_pool_size, Options, ?DEFAULT_WRITER_POOL_SIZE),
            WriterOptions = [{pool_size, WriterPoolSise} | proplists:delete(pool_size, Options)],
            case ecpool:start_sup_pool(writer_name(Producer), hstreamdb_erl_writer, WriterOptions) of
                {ok, _PidWriters} ->
                    {ok, Producer};
                {error, _} = Error -> Error
            end;
        {error, _} = Error ->
            Error
    end.

%% TODO: graceful stop
stop(Producer) ->
    ok = ecpool:stop_sup_pool(Producer),
    ok = ecpool:stop_sup_pool(writer_name(Producer)).

append(Producer, Record) ->
    ecpool:with_client(
      Producer,
      fun(Pid) ->
              gen_server:call(Pid, {append, Record})
      end).

flush(Producer) ->
    foreach_worker(
      fun(Pid) -> gen_server:call(Pid, flush) end,
      Producer).

append_flush(Producer, {PartitioningKey, Records}) when is_list(Records) ->
    ecpool:with_client(
      Producer,
      fun(Pid) ->
              gen_server:call(Pid, {append_flush, {PartitioningKey, Records}}, 60000)
      end);

append_flush(Producer, {PartitioningKey, Record}) ->
    append_flush(Producer, {PartitioningKey, [Record]}).

connect(Options) ->
    gen_server:start_link(?MODULE, Options, []).

%% -------------------------------------------------------------------------------------------------
%% gen_server part

init(Options) ->
    StreamName = proplists:get_value(stream, Options),
    Callback = proplists:get_value(callback, Options),
    MaxRecords = proplists:get_value(max_records, Options, ?DEFAULT_MAX_RECORDS),
    MaxBatches = proplists:get_value(max_batches, Options, ?DEFAULT_MAX_BATCHES),
    MaxInterval = proplists:get_value(interval, Options, ?DEFAULT_INTERVAL),
    BatchReapTimeout = proplists:get_value(
                         batch_reap_timeout, Options, ?DEFAULT_BATCH_REAP_TIMEOUT),
    ProducerName = proplists:get_value(producer_name, Options),
    CompressionType = proplists:get_value(compression_type, Options, none),
    {ok, #state{
        name = ProducerName,
        writer_name = writer_name(ProducerName),
        stream = StreamName,
        compression_type = CompressionType,
        callback = Callback,
        max_records = MaxRecords,
        max_batches = MaxBatches,
        interval = MaxInterval,
        batch_reap_timeout = BatchReapTimeout,
        record_map = #{},
        flush_deadline_map = #{},
        timer_ref = erlang:send_after(MaxInterval, self(), flush),
        key_manager = hstreamdb_key_mgr:start(Options),
        batches = #{},
        current_batches = #{}
    }}.

handle_call({append, Record}, _From, State) ->
    if_not_overflooded(
      fun() ->
              NState = do_append(Record, State),
              {reply, ok, NState}
      end,
      State);

handle_call(flush, _From, State) ->
    {reply, ok, do_flush(State, all)};

handle_call({append_flush, Records}, From, State) ->
    if_not_overflooded(
      fun() ->
              NState = do_append_sync(Records, From, State),
              {noreply, NState}
      end,
      State);

handle_call(stop, _From, State) ->
    NState = do_flush(State, all),
    {reply, ok, NState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({write_result, ShardId, BatchId, Result},
            #state{current_batches = CurrentBatches} = State) ->
    case CurrentBatches of
        #{ShardId := #batch{id = Id} = Batch} when Id =:= BatchId ->
            {noreply, handle_write_result(State, ShardId, Batch, Result)};
        #{} ->
            %% reaped batch
            {noreply, State}
    end;

handle_cast(Request, State) ->
    handle_info(Request, State).

handle_info(flush, State) ->
    {noreply, do_flush(State, deadline)};

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

foreach_worker(Fun, Pool) ->
    lists:foreach(
      fun({_Name, WPid}) ->
              ecpool_worker:exec(
                WPid,
                Fun,
                ?POOL_TIMEOUT)
      end,
      ecpool:workers(Pool)).

do_append({PartitioningKey, Record},
          State0 = #state{interval = Interval,
                         record_map = RecordMap,
                         max_records = MaxRecords,
                         key_manager = KeyManager0,
                         flush_deadline_map = FlushDeadlineMap}) ->
    {ShardId, KeyManager1} = hstreamdb_key_mgr:choose_shard(PartitioningKey, KeyManager0),
    {BatchSize, State1} = case maps:get(ShardId, RecordMap, undefined) of
                              undefined ->
                                  NRecords = [Record],
                                  NRecordMap = RecordMap#{ShardId => NRecords},
                                  NFlushDeadlineMap = FlushDeadlineMap#{
                                                        ShardId =>
                                                        Interval + erlang:monotonic_time(millisecond)
                                                       },

                                  NState = State0#state{record_map = NRecordMap,
                                                        flush_deadline_map = NFlushDeadlineMap,
                                                        key_manager = KeyManager1},
                                  {length(NRecords), NState};
                              Records ->
                                  NRecords = [Record | Records],
                                  NRecordMap = RecordMap#{ShardId => NRecords},
                                  NState = State0#state{record_map = NRecordMap,
                                                        key_manager = KeyManager1},
                                  {length(NRecords), NState}
                          end,
    case BatchSize >= MaxRecords of
        true ->
            do_flush_key(ShardId, State1);
        _ ->
            State1
    end.

do_append_sync({PartitioningKey, Records},
                From,
                State = #state{key_manager = KeyManager0}) ->
            {ShardId, KeyManager1} = hstreamdb_key_mgr:choose_shard(PartitioningKey, KeyManager0),
            Batch = batch(Records, From, State),
            append_new_batch(ShardId, Batch, State#state{key_manager = KeyManager1}).


write(ShardId, Batch, #state{writer_name = WriterName, current_batches = CurrentBatches} = State) ->
    ecpool:with_client(
      WriterName,
      fun(WriterPid) ->
            ok = hstreamdb_erl_writer:write(WriterPid, ShardId, Batch)
      end),
    State#state{
      current_batches = CurrentBatches#{ShardId => Batch}
     }.


batch_reap_deadline(#state{batch_reap_timeout = Timeout}) ->
    erlang:monotonic_time(millisecond) + Timeout.

do_flush(State = #state{flush_deadline_map = FlushDeadlineMap,
                        timer_ref = TimerRef,
                        interval = Interval}, WhichKeys) ->
    _ = cancel_timer(TimerRef),
    NTimerRef = erlang:send_after(Interval, self(), flush),
    KeysToFlush = case WhichKeys of
                      deadline ->
                          CurrentTime = erlang:monotonic_time(millisecond),
                          [ShardId || {ShardId, Deadline} <- maps:to_list(FlushDeadlineMap),
                                          Deadline < CurrentTime ];
                      all ->
                          maps:keys(FlushDeadlineMap)
                  end,
    {_Time, NState} = timer:tc(fun() -> lists:foldl(fun do_flush_key/2, State, KeysToFlush) end),
    reap_batches(NState#state{timer_ref = NTimerRef}).


reap_batches(#state{current_batches = CurrentBatches} = State) ->
    Result = {error, reaped},
    Now = erlang:monotonic_time(millisecond),
    BatchesToReap = [{ShardId, Batch}
                      || {ShardId, #batch{deadline = ReapDealine} = Batch} <- maps:to_list(CurrentBatches),
                         ReapDealine =< Now],
    lists:foldl(
      fun({ShardId, Batch}, St) ->
              handle_write_result(St, ShardId, Batch, Result)
      end,
      State,
      BatchesToReap).


cancel_timer(undefined) -> ok;
cancel_timer(TimerRef) ->
    erlang:cancel_timer(TimerRef).

do_flush_key(ShardId,
             #state{record_map = RecordMap,
                    flush_deadline_map = FlushDeadlineMap} = State0) ->
    Records = lists:reverse(maps:get(ShardId, RecordMap)),
    Batch = batch(Records, State0),
    State1 = append_new_batch(ShardId, Batch, State0),
    State1#state{record_map = maps:remove(ShardId, RecordMap),
                 flush_deadline_map = maps:remove(ShardId, FlushDeadlineMap)}.

handle_write_result(#state{current_batches = CurrentBatches,
                           stream = Stream,
                           callback = Callback} = State,
                    ShardId,
                    #batch{from = From, records = Records},
                    Result) ->
    case From of
        undefined ->
            _ = apply_callback(Callback, {{flush, Stream, length(Records)}, Result});
        _From ->
            _ = apply_callback(Callback, {{flush, Stream, length(Records)}, Result}),
            ok = gen_server:reply(From, Result)
    end,
    NState = State#state{current_batches = maps:remove(ShardId, CurrentBatches)},
    send_batch_from_queue(ShardId, NState).

apply_callback({M, F}, R) ->
    erlang:apply(M, F, [R]);
apply_callback({M, F, A}, R) ->
    erlang:apply(M, F, [R | A]);
apply_callback(F, R) ->
    F(R).

batch(Records, State) ->
    batch(Records, undefined, State).

batch(Records, From, #state{compression_type = CompressionType} = State) ->
    #batch{
       id = make_ref(),
       from = From,
       records = Records,
       compression_type = CompressionType,
       deadline = batch_reap_deadline(State)
      }.

append_new_batch(ShardId, Batch, #state{current_batches = CurrentBatches} = State) ->
    case CurrentBatches of
        #{ShardId := _Batch} ->
            enqueue_batch(ShardId, Batch, State);
        #{} ->
            write(ShardId, Batch, State)
    end.

send_batch_from_queue(ShardId,
                      #state{batches = Batches} = State) ->
    Queue = maps:get(ShardId, Batches, queue:new()),
    case queue:len(Queue) of
        0 ->
            State;
        _N ->
            {{value, Batch}, NQueue} = queue:out(Queue),
            NBatches = Batches#{ShardId => NQueue},
            write(ShardId, Batch, State#state{batches = NBatches})
    end.

enqueue_batch(ShardId, Batch, #state{batches = Batches} = State) ->
    ShardBatchesQ = maps:get(ShardId, Batches, queue:new()),
    NBatches = Batches#{ShardId => queue:in(Batch, ShardBatchesQ)},
    State#state{batches = NBatches}.

if_not_overflooded(Fun, #state{max_batches = MaxBatches} = State) ->
    PendingBatchCount = pending_batch_count(State),
    case PendingBatchCount >= MaxBatches of
        true -> {reply, {error, {overflooded, PendingBatchCount}}, State};
        false -> Fun()
    end.

pending_batch_count(#state{batches = Batches, current_batches = CurrentBatches}) ->
    lists:foldl(
      fun(Q, N) ->
              N + queue:len(Q)
      end,
      0,
      maps:values(Batches))
    + maps:size(CurrentBatches).

writer_name(ProducerName) ->
    list_to_atom(atom_to_list(ProducerName) ++ "-writer").
