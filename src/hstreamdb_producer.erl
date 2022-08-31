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
    callback,
    max_records,
    max_batches,
    interval,
    batch_reap_timeout,
    record_map,
    flush_deadline_map,
    batches,
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

append_flush(Producer, {OrderingKey, Records}) when is_list(Records) ->
    ecpool:with_client(
      Producer,
      fun(Pid) ->
              gen_server:call(Pid, {append_flush, {OrderingKey, Records}})
      end);

append_flush(Producer, {OrderingKey, Record}) ->
    append_flush(Producer, {OrderingKey, [Record]}).

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
    {ok, #state{
        name = ProducerName,
        writer_name = writer_name(ProducerName),
        stream = StreamName,
        callback = Callback,
        max_records = MaxRecords,
        max_batches = MaxBatches,
        interval = MaxInterval,
        batch_reap_timeout = BatchReapTimeout,
        record_map = #{},
        flush_deadline_map = #{}, 
        timer_ref = erlang:send_after(MaxInterval, self(), flush),
        batches = #{}
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
              NState = do_append_flush(Records, From, State),
              {noreply, NState}
      end,
      State);

handle_call(stop, _From, State) ->
    NState = do_flush(State, all),
    {reply, ok, NState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({write_result, BatchId, Result}, #state{batches = Batches} = State) ->
    case maps:get(BatchId, Batches, undefined) of
        undefined ->
            %% should not happen
            {noreply, State};
        BatchInfo ->
            {noreply, handle_write_result(State, BatchId, BatchInfo, Result)}
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

do_append({OrderingKey, Record},
          State = #state{interval = Interval,
                         record_map = RecordMap,
                         max_records = MaxRecords,
                         flush_deadline_map = FlushDeadlineMap}) ->
    case maps:get(OrderingKey, RecordMap, undefined) of
        undefined ->
            NRecordMap = RecordMap#{OrderingKey => [Record]},
            NFlushDeadlineMap = FlushDeadlineMap#{
                                  OrderingKey => Interval + erlang:monotonic_time(millisecond)
                                 },

            State#state{record_map = NRecordMap,
                        flush_deadline_map = NFlushDeadlineMap};
        Records ->
            NRecords = [Record | Records],
            NRecordMap = RecordMap#{OrderingKey => NRecords},
            NState = State#state{record_map = NRecordMap},
            case length(NRecords) >= MaxRecords of
                true ->
                    do_flush_key(OrderingKey, NState);
                _ ->
                    NState
            end
    end.

do_append_flush({OrderingKey, Records},
                From,
                State = #state{ batches = Batches
                               }) ->
            BatchId = make_ref(),
            NBatches = Batches#{BatchId => {From, length(Records), batch_reap_deadline(State)}},
            ok = write(State, BatchId, {OrderingKey, Records}),
            State#state{batches = NBatches}.

write(#state{writer_name = WriterName}, BatchId, Batch) ->
    ecpool:with_client(
      WriterName,
      fun(WriterPid) ->
            ok = hstreamdb_erl_writer:write(WriterPid, BatchId, Batch)
      end).

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
                          [OrderingKey || {OrderingKey, Deadline} <- maps:to_list(FlushDeadlineMap),
                                          Deadline < CurrentTime ];
                      all ->
                          maps:keys(FlushDeadlineMap)
                  end,
    {_Time, NState} = timer:tc(fun() -> lists:foldl(fun do_flush_key/2, State, KeysToFlush) end),
    reap_batches(NState#state{timer_ref = NTimerRef}).


reap_batches(#state{batches = Batches} = State) ->
    Result = {error, reaped},
    Now = erlang:monotonic_time(millisecond),
    BatchIdsToReap = [BatchId || {BatchId, {_From,_N, ReapDealine}} <- maps:to_list(Batches),
                                 ReapDealine < Now],
    lists:foldl(
      fun(BatchId, St) ->
              BatchInfo = maps:get(BatchId, Batches),
              handle_write_result(St, BatchId, BatchInfo, Result)
      end,
      State,
      BatchIdsToReap).


cancel_timer(undefined) -> ok;
cancel_timer(TimerRef) ->
    erlang:cancel_timer(TimerRef).

do_flush_key(OrderingKey,
             State = #state{record_map = RecordMap,
                            flush_deadline_map = FlushDeadlineMap,
                            batches = Batches}) ->
    Records = lists:reverse(maps:get(OrderingKey, RecordMap)),
    BatchId = make_ref(),
    NBatches = Batches#{BatchId => {undefined, length(Records), batch_reap_deadline(State)}},
    ok = write(State, BatchId, {OrderingKey, Records}),
    State#state{record_map = maps:remove(OrderingKey, RecordMap),
                flush_deadline_map = maps:remove(OrderingKey, FlushDeadlineMap),
                batches = NBatches}.


handle_write_result(#state{batches = Batches, stream = Stream, callback = Callback} = State,
                    BatchId, BatchInfo, Result) ->
    case BatchInfo of
        {undefined, NRecords, _ReapDeadline} ->
            _ = apply_callback(Callback, {{flush, Stream, NRecords}, Result});
        {From, NRecords, _ReapDeadline} ->
            _ = apply_callback(Callback, {{flush, Stream, NRecords}, Result}),
            ok = gen_server:reply(From, Result)
    end,
    State#state{batches = maps:remove(BatchId, Batches)}.

apply_callback({M, F}, R) ->
    erlang:apply(M, F, [R]);
apply_callback({M, F, A}, R) ->
    erlang:apply(M, F, [R | A]);
apply_callback(F, R) ->
    F(R).

if_not_overflooded(Fun, #state{batches = Batches, max_batches = MaxBatches} = State) ->
    case maps:size(Batches) >= MaxBatches of
        true -> {reply, {error, {overflooded, maps:size(Batches)}}, State};
        false -> Fun()
    end.

writer_name(ProducerName) ->
    list_to_atom(atom_to_list(ProducerName) ++ "-writer").
