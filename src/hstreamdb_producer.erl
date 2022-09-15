-module(hstreamdb_producer).

-define(DEFAULT_MAX_RECORDS, 100).
-define(DEFAULT_INTERVAL, 10).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).
-export([start/2, append/2, flush/1, stop/1, append_flush/2]).

-record(state,
        {stream,
         callback,
         max_records,
         interval,
         record_map,
         worker_pool,
         timer_ref_map,
         channel_manager}).

start(Producer, Options) ->
    gen_server:start(?MODULE, [{producer, Producer} | Options], []).

append(Producer, Record) ->
    gen_server:call(Producer, {append, Record}).

flush(Producer) ->
    gen_server:call(Producer, flush).

stop(Producer) ->
    gen_server:stop(Producer).

append_flush(Producer, Data) ->
    gen_server:call(Producer, {append_flush, Data}).

init(Options) ->
    StreamName = proplists:get_value(stream, Options),
    Callback = proplists:get_value(callback, Options),
    MaxRecords = proplists:get_value(max_records, Options, ?DEFAULT_MAX_RECORDS),
    MaxInterval = proplists:get_value(interval, Options, ?DEFAULT_INTERVAL),
    Workers = proplists:get_value(pool_size, Options, 8),
    Producer = proplists:get_value(producer, Options),
    case hstreamdb_channel_mgr:start([{pool_size, Workers} | Options]) of
        {ok, ChannelM} ->
            PoolOptions =
                [{workers, Workers},
                 {worker_type, gen_server},
                 {worker, {hstreamdb_appender, [{channel_manager, ChannelM} | Options]}}],
            case wpool:start_sup_pool(Producer, PoolOptions) of
                {ok, _Pid} ->
                    {ok,
                     #state{stream = StreamName,
                            callback = Callback,
                            max_records = MaxRecords,
                            interval = MaxInterval,
                            record_map = #{},
                            timer_ref_map = #{},
                            channel_manager = ChannelM,
                            worker_pool = Producer}};
                {error, Error} ->
                    {stop, Error}
            end;
        {error, Error} ->
            {stop, Error}
    end.

terminate(_Reason, #state{worker_pool = Producer}) ->
    _ = wpool:broadcast(Producer, stop),
    wpool:stop_sup_pool(Producer),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_info({flush, OrderingKey}, State) ->
    {noreply, do_flush(OrderingKey, State)};
handle_info(_Request, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_call({append, Record}, _From, State) ->
    case do_append(Record, State) of
        {NState, Timeout} ->
            {reply, ok, NState, Timeout};
        NState ->
            {reply, ok, NState}
    end;
handle_call(flush, _From, State) ->
    {reply, ok, do_flush(State)};
handle_call({append_flush, Records}, _From, State) ->
    NState = do_append_flush(Records, State),
    {reply, ok, NState}.

do_append({OrderingKey, Record},
          State =
              #state{interval = Interval,
                     record_map = RecordMap,
                     timer_ref_map = TimerRefMap,
                     max_records = MaxRecords}) ->
    case maps:get(OrderingKey, RecordMap, undefined) of
        undefined ->
            NTimerRefMap =
                case Interval == -1 of
                    true ->
                        TimerRefMap;
                    false ->
                        {ok, TimerRef} = timer:send_after(Interval, self(), {flush, OrderingKey}),
                        TimerRefMap#{OrderingKey => TimerRef}
                end,
            NRecordMap = RecordMap#{OrderingKey => [Record]},
            TimeoutInterval =
                case Interval == -1 of
                    true ->
                        infinity;
                    false ->
                        Interval
                end,
            {State#state{record_map = NRecordMap, timer_ref_map = NTimerRefMap}, TimeoutInterval};
        Records ->
            NRecords = [Record | Records],
            NRecordMap = RecordMap#{OrderingKey => NRecords},
            NState = State#state{record_map = NRecordMap},
            case length(NRecords) >= MaxRecords of
                true ->
                    do_flush(OrderingKey, NState);
                _ ->
                    NState
            end
    end.

do_flush(State = #state{record_map = RecordMap}) ->
    Keys = maps:keys(RecordMap),
    lists:foldl(fun do_flush/2, State, Keys).

do_flush(OrderingKey,
         State =
             #state{record_map = RecordMap,
                    stream = Stream,
                    timer_ref_map = TimerRefMap,
                    worker_pool = Workers}) ->
    Records =
        lists:reverse(
            maps:get(OrderingKey, RecordMap)),
    _ = case maps:get(OrderingKey, TimerRefMap, undefined) of
            undefined ->
                ok;
            TimerRef ->
                timer:cancel(TimerRef)
        end,
    NState = State#state{record_map = maps:remove(OrderingKey, RecordMap)},
    wpool:cast(Workers, {append, {Stream, OrderingKey, Records}}, random_worker),
    NState.

do_append_flush({OrderingKey, Record},
                #state{worker_pool = Workers, stream = Stream} = State) ->
    wpool:cast(Workers, {append, {Stream, OrderingKey, [Record]}}, random_worker),
    State.
