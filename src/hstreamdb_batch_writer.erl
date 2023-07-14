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

-module(hstreamdb_batch_writer).

-behaviour(gen_server).

-export([
    start_link/1,
    stop/1,
    write/2,
    connect/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(DEFAULT_GRPC_TIMEOUT, 30000).

-include("hstreamdb.hrl").

-record(state, {
    stream,
    grpc_timeout,
    channel_manager
}).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

write(Pid, Batch) ->
    gen_server:cast(Pid, {write, Batch, self()}).

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
    Client = proplists:get_value(client, Opts),
    GRPCTimeout = proplists:get_value(grpc_timeout, Opts, ?DEFAULT_GRPC_TIMEOUT),
    {ok, #state{
        stream = StreamName,
        grpc_timeout = GRPCTimeout,
        channel_manager = hstreamdb_channel_mgr:start(Client)
    }}.

handle_cast({write, #batch{shard_id = ShardId, id = BatchId} = Batch, Caller}, State) ->
    {Result, NState} = do_write(ShardId, records(Batch), Batch, State),
    _ = erlang:send(Caller, {write_result, ShardId, BatchId, Result}),
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

records(#batch{id = BatchId, tab = Tab}) ->
    case ets:lookup(Tab, BatchId) of
        [{_, Records}] ->
            drop_timepout(Records);
        [] ->
            []
    end.

drop_timepout(Records) ->
    Now = erlang:monotonic_time(millisecond),
    lists:flatmap(
        fun(#{deadline := Deadline, data := Data}) ->
            case Deadline of
                T when T >= Now ->
                    [Data];
                infinity ->
                    [Data];
                _ ->
                    []
            end
        end,
        Records
    ).

do_write(_ShardId, [], _Batch, State) ->
    {{ok, 0}, State};
do_write(
    ShardId,
    Records,
    #batch{compression_type = CompressionType},
    State = #state{
        channel_manager = ChannelM0,
        stream = Stream,
        grpc_timeout = GRPCTimeout
    }
) ->
    case hstreamdb_channel_mgr:lookup_client(ChannelM0, ShardId) of
        {ok, Client, ChannelM1} ->
            Req = #{
                stream_name => Stream,
                records => Records,
                shard_id => ShardId,
                compression_type => CompressionType
            },
            Options = #{
                timeout => GRPCTimeout
            },
            case hstreamdb_client:append(Client, Req, Options) of
                {ok, _} = _Res ->
                    {{ok, length(Records)}, State#state{channel_manager = ChannelM1}};
                {error, _} = Error ->
                    ChannelM2 = hstreamdb_channel_mgr:bad_client(ChannelM1, ShardId),
                    {Error, State#state{channel_manager = ChannelM2}}
            end;
        {error, _} = Error ->
            {Error, State}
    end.
