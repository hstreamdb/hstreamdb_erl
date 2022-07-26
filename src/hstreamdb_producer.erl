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
-define(DEFAULT_INTERVAL, 10).

-behaviour(gen_server).

-export([ start/2
        , stop/1
        , append/2
        , flush/1
        , append_flush/2
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-record(state, {
    stream,
    callback,
    max_records,
    interval,
    record_map,
    channel_manager,
    timer_ref
}).

start(Producer, Options) ->
    Workers = proplists:get_value(pool_size, Options, 8),
    PoolOptions = [
        {workers, Workers},
        {worker_type, gen_server},
        {worker, {?MODULE, Options}}
    ],
    case wpool:start_sup_pool(Producer, PoolOptions) of
        {ok, _Pid} ->
            {ok, Producer};
        {error, Error} ->
            {error, Error}
    end.

stop(Producer) ->
    _ = wpool:broadcast(Producer, stop),
    wpool:stop_sup_pool(Producer).

append(Producer, Record) ->
    wpool:call(Producer, {append, Record}).

flush(Producer) ->
    wpool:call(Producer, flush).

append_flush(Producer, Records) ->
    wpool:call(Producer, {append_flush, Records}).

%% -------------------------------------------------------------------------------------------------
%% gen_server part

init(Options) ->
    StreamName = proplists:get_value(stream, Options),
    Callback = proplists:get_value(callback, Options),
    MaxRecords = proplists:get_value(max_records, Options, ?DEFAULT_MAX_RECORDS),
    MaxInterval = proplists:get_value(interval, Options, ?DEFAULT_INTERVAL),
    {ok, #state{
        stream = StreamName,
        callback = Callback,
        max_records = MaxRecords,
        interval = MaxInterval,
        record_map = #{},
        channel_manager = hstreamdb_channel_mgr:start(Options)
    }}.

handle_call({append, Record}, _From, State) ->
    case do_append(Record, State) of
        {NState, Timeout} ->
            {reply, ok, NState ,Timeout};
        NState ->
            {reply, ok, NState}
    end;

handle_call(flush, _From, State) ->
    {reply, ok, do_flush(State)};

handle_call({append_flush, Records}, _From, State) ->
    {Res, NState} = do_append_flush(Records, State),
    {reply, Res, NState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(stop, State = #state{channel_manager = ChannelM}) ->
    NState = do_flush(State),
    ok = hstreamdb_channel_mgr:stop(ChannelM),
    {noreply, NState#state{channel_manager = #{}}};

handle_cast(Request, State) ->
    handle_info(Request, State).

handle_info(flush, State) ->
    {noreply, do_flush(State)};

handle_info(timeout, State) ->
    {noreply, do_flush(State)};

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

do_append({OrderingKey, Record}, State = #state{interval = Interval,
                                 record_map = RecordMap,
                                 max_records = MaxRecords}) ->
    case maps:get(OrderingKey, RecordMap, undefined) of
        undefined ->
            {ok, TimerRef} = timer:send_after(Interval, self(), flush),
            NRecordMap = RecordMap#{OrderingKey => [Record]},
            {State#state{record_map = NRecordMap, timer_ref = TimerRef}, Interval};
        Records ->
            NRecords = [Record | Records],
            NRecordMap = RecordMap#{OrderingKey => NRecords},
            NState = State#state{record_map = NRecordMap},
            case length(NRecords) >= MaxRecords of
                true ->
                    do_flush(NState);
                _ ->
                    NState
            end
    end.

do_flush(State = #state{record_map = RecordMap}) ->
    Keys = maps:keys(RecordMap),
    lists:foldl(fun do_flush/2, State, Keys).

do_flush(OrderingKey, State = #state{record_map = RecordMap,
                                     stream = Stream,
                                     channel_manager = ChannelM,
                                     timer_ref = TimerRef,
                                     callback = Callback}) ->
    Records = lists:reverse(maps:get(OrderingKey, RecordMap)),
    _ = timer:cancel(TimerRef),
    NState = State#state{record_map = maps:remove(OrderingKey, RecordMap)},
    case hstreamdb_channel_mgr:lookup_channel(OrderingKey, ChannelM) of
        {ok, Channel} ->
            do_flush(Stream,
                     OrderingKey,
                     Records,
                     Channel,
                     Callback,
                     NState);
        {ok, Channel, NCManager} ->
            do_flush(Stream,
                     OrderingKey,
                     Records,
                     Channel,
                     Callback,
                     NState#state{channel_manager = NCManager});
        {error, Error} ->
            _ = apply_callback(Callback, {{flush, Stream, Records}, {error, Error}}),
            NState
    end.

do_flush(Stream, OrderingKey, Records, Channel, Callback, State = #state{channel_manager = CMgr}) ->
    Res = flush_request(Stream, Records, Channel),
    _ = apply_callback(Callback, {{flush, Stream, Records}, Res}),
    case Res of
        {ok, _Resp} ->
            State;
        _Error ->
            NCManager = hstreamdb_channel_mgr:bad_channel(OrderingKey, CMgr),
            State#state{channel_manager = NCManager}
    end.

do_append_flush({OrderingKey, Records},
 State = #state{stream = Stream, channel_manager = ChannelM}) when is_list(Records) ->
    case hstreamdb_channel_mgr:lookup_channel(OrderingKey, ChannelM) of
        {ok, Channel} ->
            case flush_request(Stream, Records, Channel) of
                Res = {ok, _} ->
                    {Res, State};
                Error ->
                    NCManager = hstreamdb_channel_mgr:bad_channel(OrderingKey, ChannelM),
                    {Error, State#state{channel_manager = NCManager}}
            end;
        {ok, Channel, NCManager} ->
            case flush_request(Stream, Records, Channel) of
                Res = {ok, _} ->
                    {Res, State#state{channel_manager = NCManager}};
                Error ->
                    NCManager = hstreamdb_channel_mgr:bad_channel(OrderingKey, ChannelM),
                    {Error, State#state{channel_manager = NCManager}}
            end;
        {error, Error} ->
            {{error, Error}, State}
    end;

do_append_flush({OrderingKey, Record}, State) ->
    do_append_flush({OrderingKey, [Record]}, State).

flush_request(Stream, Records, Channel) ->
    Req = #{streamName => Stream, records => Records},
    Options = #{channel => Channel},
    case hstreamdb_client:append(Req, Options) of
        {ok, Resp, _MetaData} ->
            {ok, Resp};
        {error, R} ->
            {error, R}
    end.

apply_callback({M, F}, R) ->
    erlang:apply(M, F, [R]);
apply_callback({M, F, A}, R) ->
    erlang:apply(M, F, [R | A]);
apply_callback(F, R) ->
    F(R).
