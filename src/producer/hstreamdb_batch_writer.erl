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
    on_shards_updated/3,
    on_init/1,
    on_terminate/4
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

%%--------------------------------------------------------------------------------------------------
%% ecpool part
%%--------------------------------------------------------------------------------------------------

-type options() :: #{
    name := ecpool:pool_name(),
    stream := hstreamdb:stream(),
    grpc_timeout => non_neg_integer()
}.

-spec start_link(options()) -> gen_server:start_ret().
connect(PoolOptions) ->
    Options = proplists:get_value(opts, PoolOptions),
    start_link(Options).

%%-------------------------------------------------------------------------------------------------
%% Discovery
%%-------------------------------------------------------------------------------------------------

-define(SHARD_CLIENT_KEY(NAME, SHARD_ID), {?MODULE, NAME, SHARD_ID}).

on_init(Name) -> cleanup(Name).
on_terminate(Name, _Vsn, _KeyManager, _ShardClientMgr) -> cleanup(Name).

on_shards_updated(Name, {OldVsn, _OldKeyMgr, _OldClientMgr}, {NewVsn, NewKeyMgr, NewClientMgr}) ->
    % ct:print("on_shards_updated, name: ~p, old_vsn: ~p, new_vsn: ~p~n", [Name, OldVsn, NewVsn]),
    % ct:print("on_shards_updated, NewClientMgr: ~p~n", [NewClientMgr]),
    {ok, ShardIds} = hstreamdb_key_mgr:shard_ids(NewKeyMgr),
    ok = lists:foreach(
        fun(ShardId) ->
            {ok, ShardClient, _ClientMgr} = hstreamdb_shard_client_mgr:lookup_shard_client(NewClientMgr, ShardId),
            ok = set_shard_client(Name, ShardId, NewVsn, ShardClient)
        end,
        ShardIds
    ),
    true = ets:match_delete(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(Name, '_'), {OldVsn, '_'}}),
    % ct:print("discovery tab: ~p", [ets:tab2list(?DISCOVERY_TAB)]),
    ok.

cleanup(Name) ->
    true = ets:match_delete(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(Name, '_'), '_'}),
    ok.

shard_client(Name, ShardId) ->
    case ets:lookup(?DISCOVERY_TAB, ?SHARD_CLIENT_KEY(Name, ShardId)) of
        [] ->
            not_found;
        [{_, {Version, ShardClient}}] ->
            {ok, Version, ShardClient}
    end.

set_shard_client(Name, ShardId, Version, ShardClient) ->
    true = ets:insert(?DISCOVERY_TAB, {
        ?SHARD_CLIENT_KEY(Name, ShardId), {Version, ShardClient}
    }),
    ok.

%%-------------------------------------------------------------------------------------------------
%% gen_server
%%-------------------------------------------------------------------------------------------------

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

handle_cast({write, #batch{shard_id = ShardId} = Batch, Caller}, State) ->
    {Result, NState} = do_write(ShardId, records(Batch), Batch, State),
    % ct:print("writer, caller: ~p, result ready~n", [Caller]),
    % ct:print("writer, caller is ~p~n", [sys:get_state(Caller)]),
    _ = erlang:send(Caller, {write_result, Batch, Result}),
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

records(#batch{batch_ref = BatchId, tab = Tab}) ->
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
            fun(BufferRecord) ->
                case hstreamdb_buffer:deadline(BufferRecord) of
                    infinity ->
                        {undefined, hstreamdb_buffer:data(BufferRecord)};
                    T when T >= Now ->
                        {undefined, hstreamdb_buffer:data(BufferRecord)};
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
    case shard_client(Name, ShardId) of
        {ok, Version, Client} ->
            % ct:print("do_write, version: ~p, records: ~p~n", [Version, Records]),
            Req = #{
                stream_name => Stream,
                records => Records,
                shard_id => ShardId,
                compression_type => CompressionType
            },
            Options = #{
                timeout => GRPCTimeout
            },
            try hstreamdb_client:append(Client, Req, Options) of
                {ok, #{recordIds := RecordsIds}} ->
                    Res = fill_responses(Resps, RecordsIds),
                    {{ok, Res}, State};
                {error, Reason} = Error ->
                    hstreamdb_discovery:report_shard_unavailable(Name, Version, ShardId, Reason),
                    {Error, State}
            catch
                error:badarg ->
                    {{error, cannot_resolve_shard_id}, State}
            end;
        not_found ->
            % ct:print("do_write, not_found, records: ~p~n", [Records]),
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
