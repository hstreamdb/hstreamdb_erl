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

-export_type([options/0]).

-define(DEFAULT_GRPC_TIMEOUT, 30000).

-include("hstreamdb.hrl").

-record(state, {
    name,
    stream,
    grpc_timeout
}).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

write(Pid, Batch) ->
    gen_server:cast(Pid, {write, Batch, self()}).

stop(Pid) ->
    gen_server:call(Pid, stop).

%% -------------------------------------------------------------------------------------------------
%% ecpool part

-type options() :: #{
    name := ecpool:pool_name(),
    stream := hstreamdb:stream(),
    grpc_timeout => non_neg_integer()
}.

-spec start_link(options()) -> gen_server:start_ret().
connect(PoolOptions) ->
    Options = proplists:get_value(opts, PoolOptions),
    start_link(Options).

%% -------------------------------------------------------------------------------------------------
%% gen_server part

init([Opts]) ->
    process_flag(trap_exit, true),
    StreamName = maps:get(stream, Opts),
    Name = maps:get(name, Opts),
    GRPCTimeout = maps:get(grpc_timeout, Opts, ?DEFAULT_GRPC_TIMEOUT),
    {ok, #state{
        stream = StreamName,
        name = Name,
        grpc_timeout = GRPCTimeout
    }}.

handle_cast({write, #batch{shard_id = ShardId, id = BatchId} = Batch, Caller}, State) ->
    {Result, NState} = do_write(ShardId, records(Batch), Batch, State),
    _ = erlang:send(Caller, {write_result, ShardId, BatchId, Result}),
    {noreply, NState}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

records(#batch{id = BatchId, tab = Tab}) ->
    case ets:lookup(Tab, BatchId) of
        [{_, Records}] ->
            prepare_known_resps(Records);
        [] ->
            {[], []}
    end.

prepare_known_resps(Records) ->
    Now = erlang:monotonic_time(millisecond),
    {Resps, Reqs} = lists:unzip(
        lists:map(
            fun(#{deadline := Deadline, data := Data}) ->
                case Deadline of
                    T when T >= Now ->
                        {undefined, Data};
                    infinity ->
                        {undefined, Data};
                    _ ->
                        {{error, timeout}, undefined}
                end
            end,
            Records
        )
    ),
    {Resps, lists:filter(fun(X) -> X =/= undefined end, Reqs)}.

do_write(_ShardId, {Resps, []}, _Batch, State) ->
    {{ok, Resps}, State};
do_write(
    ShardId,
    {Resps, Records},
    #batch{compression_type = CompressionType},
    State = #state{
        name = Name,
        stream = Stream,
        grpc_timeout = GRPCTimeout
    }
) ->
    case hstreamdb_discovery:shard_client(Name, ShardId) of
        {ok, Client} ->
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
                {ok, #{recordIds := RecordsIds}} ->
                    Res = fill_responses(Resps, RecordsIds),
                    {{ok, Res}, State};
                {error, _} = Error ->
                    %% TODO
                    %% report node unavail
                    {Error, State}
            end;
        not_found ->
            {{error, cannot_resolve_shard_id}, State}
    end.

fill_responses(KnownResps, Ids) ->
    fill_responses(KnownResps, Ids, []).

fill_responses([], [], Acc) ->
    lists:reverse(Acc);
fill_responses([undefined | Rest], [Id | Ids], Acc) ->
    fill_responses(Rest, Ids, [{ok, Id} | Acc]);
fill_responses([Resp | Rest], Ids, Acc) ->
    fill_responses(Rest, Ids, [Resp | Acc]).
