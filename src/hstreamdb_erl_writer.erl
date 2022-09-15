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

-module(hstreamdb_erl_writer).

-behaviour(gen_server).

-export([ start_link/1
        , stop/1
        , write/3
        , connect/1
        ]).

-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(DEFAULT_GRPC_TIMEOUT, 30000).

-include("hstreamdb.hrl").

-record(state, {
    stream,
    grpc_timeout,
    ordkey_manager,
    channel_manager
}).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

write(Pid, BatchId, Records) ->
    gen_server:cast(Pid, {write, BatchId, Records, self()}).

stop(Pid) ->
    gen_server:call(Pid, stop).

%% -------------------------------------------------------------------------------------------------
%% ecpool part

connect(Opts) ->
    start_link(Opts).

%% -------------------------------------------------------------------------------------------------
%% gen_server part

init([Opts]) ->
    process_flag(trap_exit, true),
    StreamName = proplists:get_value(stream, Opts),
    GRPCTimeout = proplists:get_value(grpc_timeout, Opts, ?DEFAULT_GRPC_TIMEOUT),
    {ok, #state{
            stream = StreamName,
            grpc_timeout = GRPCTimeout,
            ordkey_manager = hstreamdb_ordkey_mgr:start(Opts),
            channel_manager = hstreamdb_channel_mgr:start(Opts)
           }}.

handle_cast({write, BatchId, Batch, Caller}, State) ->
    {Result, NState} = do_write(Batch, State),
    ok = gen_server:cast(Caller, {write_result, BatchId, Result}),
    {noreply, NState}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{channel_manager = ChannelM}) ->
    ok = hstreamdb_channel_mgr:stop(ChannelM),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

do_write({OrderingKey, Records},
          State = #state{channel_manager = ChannelM0,
                         ordkey_manager = OrderingKeyM0,
                         stream = Stream,
                         grpc_timeout = GRPCTimeout}) ->
    case hstreamdb_ordkey_mgr:lookup_channel(OrderingKey, OrderingKeyM0) of
        {ok, {ShardId, Node}} ->
            case hstreamdb_channel_mgr:lookup_channel(Node, ChannelM0) of
                {ok, Channel, ChannelM1} ->
                    Req = #{streamName => Stream, records => Records, shardId => ShardId},
                    Options = #{channel => Channel, timeout => GRPCTimeout},
                    case flush(OrderingKey, Req, Options) of
                        {ok, _} = Res ->
                            {Res, State#state{channel_manager = ChannelM1}};
                        {error, _} = Error ->
                            ChannelM2 = hstreamdb_channel_mgr:bad_channel(Channel, ChannelM1),
                            {Error, State#state{channel_manager = ChannelM2}}
                    end;
                {error, _} = Error ->
                    {Error, State}
            end;
        {error, _} = Error ->
            {Error, State}
    end.

flush(OrderingKey,
      #{records := Records} = Req,
      #{channel := Channel, timeout := Timeout} = Options) ->
    case timer:tc(fun() -> ?HSTREAMDB_CLIENT:append(Req, Options) end) of
        {Time, {ok, Resp, _MetaData}} ->
            logger:info("flush_request[~p, ~p], pid=~p, SUCCESS, ~p records in ~p ms~n", [Channel, OrderingKey, self(), length(Records), Time div 1000]),
            {ok, Resp};
        {Time, {error, R}} ->
            logger:error("flush_request[~p, ~p], pid=~p, timeout=~p, ERROR: ~p, in ~p ms~n", [Channel, OrderingKey, self(), Timeout, R, Time div 1000]),
            {error, R}
    end.

