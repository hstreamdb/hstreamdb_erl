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

-module(hstreamdb_discovery).

-include("hstreamdb.hrl").

-behaviour(gen_statem).

-export([
    start_link/1,
    key_manager/1,
    shard_client/2
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    code_change/4
]).

-type options() :: #{
    stream := hstreamdb:stream(),
    client_options := hstreamdb_client:options(),
    name := ecpool:pool_name(),
    backoff_options => hstreamdb_backoff:options(),
    min_active_time => non_neg_integer()
}.

-export_type([options/0]).

-type data() :: #{
    name := ecpool:pool_name(),
    stream := hstreamdb:stream(),
    backoff_options := hstreamdb_backoff:options(),
    client := hstreamdb:client(),
    min_active_time := non_neg_integer()
}.

-define(discovering(BACKOFF), {discovering, BACKOFF}).
-define(active(DEADLINE), {active, DEADLINE}).

-type deadline() :: integer().
-type state() :: ?discovering(hstreamdb_backoff:t()) | ?active(deadline()).

-define(KEY_MANAGER_KEY(DISCOVERY_KEY), {DISCOVERY_KEY, key_manager}).
-define(SHARD_CLIENT_KEY(DISCOVERY_KEY, SHARD_ID), {DISCOVERY_KEY, shard_client, SHARD_ID}).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

start_link(Options) ->
    gen_statem:start_link(?MODULE, [Options], []).

key_manager(Name) ->
    DiscoveryKey = discovery_key(Name),
    case ets:lookup(?DISCOVERY_TAB, ?KEY_MANAGER_KEY(DiscoveryKey)) of
        [] ->
            not_found;
        [{_, KeyManager}] ->
            {ok, KeyManager}
    end.

shard_client(Name, ShardId) ->
    DiscoveryKey = discovery_key(Name),
    case ets:lookup(?DISCOVERY_TAB, ?SHARD_CLIENT_KEY(DiscoveryKey, ShardId)) of
        [] ->
            not_found;
        [{_, ShardClient}] ->
            {ok, ShardClient}
    end.

%%-------------------------------------------------------------------------------------------------
%% gen_statem callbacks
%%-------------------------------------------------------------------------------------------------

callback_mode() ->
    handle_event_function.

-spec init(options()) -> {ok, state(), data(), [gen_statem:action()]} | {error, term()}.
init([Options]) ->
    _ = process_flag(trap_exit, true),

    StreamName = maps:get(stream, Options),
    ClientOptions0 = maps:get(client_options, Options),
    %% We need original RPC options for creating clients for each shard.
    %% But for shard lookup we need synchronous exclusive client, so we override
    %% the pool size to 1
    RPCOptions = maps:get(rpc_options, ClientOptions0, #{}),
    ClientOptions1 = ClientOptions0#{rpc_options => RPCOptions#{pool_size => 1}},
    BackoffOptions = maps:get(backoff_options, Options, ?DEFAULT_BATCH_BACKOFF_OPTIONS),
    Name = maps:get(name, Options),
    MinActiveTime = maps:get(min_active_time, Options, ?DEFAULT_MIN_ACTIVE_TIME),

    case hstreamdb_client:create(ClientOptions1) of
        {ok, Client} ->
            Data = #{
                name => Name,
                stream => StreamName,
                backoff_options => BackoffOptions,
                rpc_options => RPCOptions,
                client => Client,
                min_active_time => MinActiveTime
            },
            {ok, ?discovering(backoff(Data)), Data, [{state_timeout, 0, discover}]};
        {error, _} = Error ->
            Error
    end.

%% Discovering

handle_event(state_timeout, discover, ?discovering(Backoff0), #{stream := Stream} = Data) ->
    Pipeline = [
        {fun reconnect/2, []},
        {fun list_shards/2, []},
        {fun create_clients/2, []},
        {fun store_clients/2, []}
    ],
    case pipeline(Pipeline, Data) of
        {ok, _} ->
            {next_state, ?active(may_discover_deadline(Data)), Data};
        {error, Reason} ->
            logger:error("[hstreamdb] Failed to update shards for stream ~p: ~p", [Stream, Reason]),
            {Delay, Backoff1} = hstreamdb_backoff:next_delay(Backoff0),
            {next_state, ?discovering(Backoff1), Data, [{state_timeout, Delay, discover}]}
    end;
handle_event(cast, node_unavailable, ?discovering(_), _Data) ->
    keep_state_and_data;

%% Active

%% After having become active, we ignore `node_unavailable` messages for some time
%% * to get them drained
%% * to ignore late reports
handle_event(cast, node_unavailable, ?active(MayDiscoverDeadline), Data) ->
    case MayDiscoverDeadline < erlang:monotonic_time(millisecond) of
        true ->
            ok = delete_clients(Data),
            {next_state, ?discovering(backoff(Data)), Data, [{state_timeout, 0, discover}]};
        false ->
            keep_state_and_data
    end;

%% Fallbacks

handle_event({call, From}, Request, _State, _Data) ->
    {keep_state_and_data, [{reply, From, {error, {unknown_call, Request}}}]};
handle_event(info, _Request, _State, _Data) ->
    keep_state_and_data;
handle_event(cast, _Request, _State, _Data) ->
    keep_state_and_data.

terminate(_Reason, _State, Data) ->
    DiscoveryKey = discovery_key(Data),
    true = ets:delete(?DISCOVERY_TAB, ?KEY_MANAGER_KEY(DiscoveryKey)),
    ok = delete_clients(Data),
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%-------------------------------------------------------------------------------------------------
%% Internal functions
%%-------------------------------------------------------------------------------------------------

%% The initial value for Ctx is data()
reconnect([], #{client := Client} = Ctx) ->
    case hstreamdb_client:reconnect(Client) of
        ok ->
            {ok, Ctx#{client => Client}};
        {error, _} = Error ->
            Error
    end.

list_shards([], #{client := Client, stream := Stream} = Ctx) ->
    KeyManager = hstreamdb_key_mgr:create(Stream),
    case hstreamdb_key_mgr:update_shards(Client, KeyManager) of
        {ok, KeyManager1} ->
            {ok, Ctx#{key_manager => KeyManager1}};
        {error, _} = Error ->
            Error
    end.

create_clients(
    [], #{client := Client, key_manager := KeyManager, rpc_options := OverrideRPCOptions} = Ctx
) ->
    {ok, ShardIds} = hstreamdb_key_mgr:shard_ids(KeyManager),
    ShardClientManager = hstreamdb_shard_client_mgr:start(Client, #{
        cache_by_shard_id => true,
        client_override_opts => OverrideRPCOptions
    }),
    Pipeline = lists:map(
        fun(ShardId) ->
            {fun cache_shard_client/2, ShardId}
        end,
        ShardIds
    ),
    pipeline(Pipeline, Ctx#{shard_client_manager => ShardClientManager, shard_clients => []}).

cache_shard_client(
    ShardId, #{shard_client_manager := ShardClientManager0, shard_clients := ShardClients} = Ctx
) ->
    case hstreamdb_shard_client_mgr:lookup_shard_client(ShardClientManager0, ShardId) of
        {ok, ShardClient, ShardClientManager1} ->
            {ok, Ctx#{
                shard_client_manager => ShardClientManager1,
                shard_clients => [{ShardId, ShardClient} | ShardClients]
            }};
        {error, _} = Error ->
            Error
    end.

store_clients(
    [],
    #{
        shard_clients := ShardClients,
        key_manager := KeyManager
    } = Ctx
) ->
    DiscoveryKey = discovery_key(Ctx),
    true = ets:insert(?DISCOVERY_TAB, {?KEY_MANAGER_KEY(DiscoveryKey), KeyManager}),
    ok = lists:foreach(
        fun({ShardId, ShardClient}) ->
            true = ets:insert(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(DiscoveryKey, ShardId), ShardClient})
        end,
        ShardClients
    ).

delete_clients(Data) ->
    DiscoveryKey = discovery_key(Data),
    true = ets:match_delete(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(DiscoveryKey, '_'), '_'}),
    ok = lists:foreach(
        fun({_, ShardClient}) ->
            ok = hstreamdb_client:stop(ShardClient)
        end,
        ets:match(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(DiscoveryKey, '_'), '_'})
    ).

pipeline([], Result) ->
    {ok, Result};
pipeline([{Fun, Arg} | Rest], Result0) ->
    try Fun(Arg, Result0) of
        ok ->
            pipeline(Rest, Result0);
        {ok, Resuslt1} ->
            pipeline(Rest, Resuslt1);
        {error, _} = Error ->
            Error
    catch
        Error:Reason:Stacktrace ->
            {error, {Error, Reason, Stacktrace}}
    end.

discovery_key(#{name := Name}) ->
    Name;
discovery_key(Name) when is_atom(Name) orelse is_binary(Name) ->
    Name.

backoff(#{backoff_options := BackoffOptions}) ->
    hstreamdb_backoff:new(BackoffOptions).

may_discover_deadline(#{min_active_time := MinActiveTime}) ->
    erlang:monotonic_time(millisecond) + MinActiveTime.
