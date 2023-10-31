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
    report_shard_unavailable/4
]).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    terminate/3,
    code_change/4
]).

-type name() :: ecpool:pool_name().

-type init_callback() :: fun((name()) -> any()).

-type stream_update_callback() :: fun(
    (name(), {version(), hstreamdb_key_mgr:t()}, {version(), hstreamdb_key_mgr:t()}) -> any()
).

-type shard_update_callback() :: fun(
    (
        name(),
        {version(), hstreamdb_key_mgr:t(), hstreamdb_shard_client_mgr:t()},
        {version(), hstreamdb_key_mgr:t(), hstreamdb_shard_client_mgr:t()}
    ) -> any()
).

-type terminate_callback() :: fun(
    (name(), version(), hstreamdb_key_mgr:t(), hstreamdb_shard_client_mgr:t()) -> any()
).

-type options() :: #{
    stream := hstreamdb:stream(),
    client_options := hstreamdb_client:options(),
    name := name(),
    backoff_options => hstreamdb_backoff:options(),
    min_active_time => non_neg_integer(),

    on_init => init_callback(),
    on_stream_updated => stream_update_callback(),
    on_shards_updated => shard_update_callback(),
    on_terminate => terminate_callback()
}.

-export_type([options/0]).

-type data() :: #{
    name := name(),
    stream := hstreamdb:stream(),
    backoff_options := hstreamdb_backoff:options(),
    client := hstreamdb:client(),
    rpc_options := hstreamdb_client:rpc_options(),

    shard_client_manager := hstreamdb_shard_client_mgr:t() | undefined,
    key_manager := hstreamdb_key_mgr:t() | undefined,
    version := version() | undefined,

    on_init => init_callback(),
    on_shards_updated := shard_update_callback(),
    on_stream_updated := stream_update_callback(),
    on_terminate := terminate_callback()
}.

-type version() :: reference().

-define(discovering(BACKOFF), {discovering, BACKOFF}).
-define(active, active).

-type state() :: ?discovering(hstreamdb_backoff:t()) | ?active.

-define(ID(NAME), {?MODULE, NAME}).

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

-spec start_link(options()) -> {ok, pid()} | {error, term()}.
start_link(#{name := Name} = Options) ->
    gen_statem:start_link(?VIA_GPROC(?ID(Name)), ?MODULE, [Options], []).

-spec report_shard_unavailable(name(), version(), hstreamdb_client:shard_id(), term()) -> ok.
report_shard_unavailable(Name, Version, ShardId, _Reason) ->
    logger:error("[hstreamdb] Shard ~p is unavailable for stream ~p, version ~p", [
        ShardId, Name, Version
    ]),
    gen_statem:cast(?VIA_GPROC(?ID(Name)), {shard_unavailable, ShardId, Version}).

%%-------------------------------------------------------------------------------------------------
%% gen_statem callbacks
%%-------------------------------------------------------------------------------------------------

callback_mode() ->
    handle_event_function.

-spec init([options()]) -> {ok, state(), data(), [gen_statem:action()]} | {stop, term()}.
init([Options]) ->
    _ = process_flag(trap_exit, true),

    Name = maps:get(name, Options),
    StreamName = maps:get(stream, Options),
    ClientOptions0 = maps:get(client_options, Options),
    %% We need original RPC options for creating clients for each shard.
    %% But for shard lookup we need synchronous exclusive client, so we override
    %% the pool size to 1
    RPCOptions = maps:get(rpc_options, ClientOptions0, #{}),
    ClientOptions1 = ClientOptions0#{rpc_options => RPCOptions#{pool_size => 1}},
    BackoffOptions = maps:get(backoff_options, Options, ?DEFAULT_DISOVERY_BACKOFF_OPTIONS),

    OnInit = maps:get(on_init, Options),
    OnStreamUpdated = maps:get(on_stream_updated, Options),
    OnShardsUpdated = maps:get(on_shards_updated, Options),
    OnTerminate = maps:get(on_terminate, Options),

    case hstreamdb_client:create(ClientOptions1) of
        {ok, Client} ->
            Data = #{
                name => Name,
                stream => StreamName,
                backoff_options => BackoffOptions,
                rpc_options => RPCOptions,
                client => Client,

                shard_client_manager => undefined,
                key_manager => undefined,
                version => undefined,

                on_init => OnInit,
                on_shards_updated => OnShardsUpdated,
                on_stream_updated => OnStreamUpdated,
                on_terminate => OnTerminate
            },
            ok = run_callbacks(on_init, [Name], Data),
            % ct:print("hstreamdb_discovery discovery started"),
            {ok, ?discovering(backoff(Data)), Data, [{state_timeout, 0, discover}]};
        {error, Reason} ->
            {stop, Reason}
    end.

%% Discovering

handle_event(state_timeout, discover, ?discovering(Backoff0), #{stream := Stream} = Data0) ->
    Pipeline = [
        {fun reconnect/2, []},
        {fun list_shards/2, []},
        {fun create_clients/2, []},
        {fun call_callbacks/2, []},
        {fun stop_old_shard_clients/2, []}
    ],
    case pipeline(Pipeline, Data0) of
        {ok, #{
            new_key_manager := NewKeyManager,
            new_shard_client_manager := ShardClientManager,
            new_version := Version
        }} ->
            Data1 = Data0#{
                key_manager => NewKeyManager,
                shard_client_manager => ShardClientManager,
                version => Version
            },
            % ct:print("hstreamdb_discovery discovery succeeded"),
            {next_state, ?active, Data1};
        {error, Reason} ->
            logger:error("[hstreamdb] Failed to update shards for stream ~p: ~p", [Stream, Reason]),
            {Delay, Backoff1} = hstreamdb_backoff:next_delay(Backoff0),
            {next_state, ?discovering(Backoff1), Data0, [{state_timeout, Delay, discover}]}
    end;
handle_event(cast, {shard_unavailable, _ShardId, _Version}, ?discovering(_), _Data) ->
    keep_state_and_data;
%% Active

handle_event(cast, {shard_unavailable, _ShardId, Version}, ?active, #{version := Version} = Data0) ->
    Data1 = stop_shard_clients(Data0),
    {next_state, ?discovering(backoff(Data1)), Data1, [{state_timeout, 0, discover}]};
%% Stale notifications
handle_event(cast, {shard_unavailable, _ShardId, _Version}, ?active, _Data) ->
    keep_state_and_data;
%% Fallbacks

handle_event({call, From}, Request, _State, _Data) ->
    {keep_state_and_data, [{reply, From, {error, {unknown_call, Request}}}]};
handle_event(info, _Request, _State, _Data) ->
    keep_state_and_data;
handle_event(cast, _Request, _State, _Data) ->
    keep_state_and_data.

terminate(
    _Reason,
    _State,
    #{
        name := Name,
        key_manager := KeyManager,
        shard_client_manager := ShardClientManager,
        version := Version
    } = Data
) ->
    ok = run_callbacks(on_terminate, [Name, Version, KeyManager, ShardClientManager], Data),
    _Data = stop_shard_clients(Data),
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
    NewKeyManager0 = hstreamdb_key_mgr:create(Stream),
    case hstreamdb_key_mgr:update_shards(Client, NewKeyManager0) of
        {ok, NewKeyManager1} ->
            {ok, Ctx#{
                new_key_manager => NewKeyManager1
            }};
        {error, _} = Error ->
            Error
    end.

create_clients(
    [],
    #{client := Client, new_key_manager := NewKeyManager, rpc_options := OverrideRPCOptions} = Ctx
) ->
    {ok, ShardIds} = hstreamdb_key_mgr:shard_ids(NewKeyManager),
    NewShardClientManager = hstreamdb_shard_client_mgr:start(Client, #{
        cache_by_shard_id => true,
        client_override_opts => OverrideRPCOptions
    }),
    Pipeline = lists:map(
        fun(ShardId) ->
            {fun cache_shard_client/2, ShardId}
        end,
        ShardIds
    ),
    pipeline(Pipeline, Ctx#{new_shard_client_manager => NewShardClientManager}).

cache_shard_client(
    ShardId, #{new_shard_client_manager := NewShardClientManager0} = Ctx
) ->
    case hstreamdb_shard_client_mgr:lookup_shard_client(NewShardClientManager0, ShardId) of
        {ok, _ShardClient, NewShardClientManager1} ->
            {ok, Ctx#{
                new_shard_client_manager => NewShardClientManager1
            }};
        {error, _} = Error ->
            Error
    end.

call_callbacks(
    [],
    #{
        name := Name,
        key_manager := KeyManager,
        new_key_manager := NewKeyManager,
        shard_client_manager := ShardClientManager,
        new_shard_client_manager := NewShardClientManager,
        version := Version
    } = Ctx
) ->
    NewVersion = make_ref(),
    ok = run_callbacks(
        on_shards_updated,
        [
            Name,
            {Version, KeyManager, ShardClientManager},
            {NewVersion, NewKeyManager, NewShardClientManager}
        ],
        Ctx
    ),
    ok = run_callbacks(
        on_stream_updated,
        [Name, {Version, KeyManager}, {NewVersion, NewKeyManager}],
        Ctx
    ),
    {ok, Ctx#{new_version => NewVersion}}.

stop_old_shard_clients([], Ctx) ->
    {ok, stop_shard_clients(Ctx)}.

stop_shard_clients(#{shard_client_manager := undefined} = Data) ->
    Data;
stop_shard_clients(#{shard_client_manager := ShardClientManager} = Data) ->
    ok = hstreamdb_shard_client_mgr:stop(ShardClientManager),
    Data#{shard_client_manager => undefined}.

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

run_callbacks(Name, Args, Data) ->
    Funs = maps:get(Name, Data),
    lists:foreach(
        fun(Fun) ->
            safe_call(Name, Fun, Args)
        end,
        Funs
    ).

safe_call(Name, Fun, Args) ->
    try
        apply(Fun, Args)
    catch
        Error:Reason:Stacktrace ->
            % ct:print("hstreamdb_discovery callback ~p failed: ~p", [
            %     Name, {Error, Reason, Stacktrace}
            % ]),
            logger:error("[hstreamdb] Failed to call discovery callback ~p: ~p", [
                Name, {Error, Reason, Stacktrace}
            ])
    end,
    ok.

backoff(#{backoff_options := BackoffOptions}) ->
    hstreamdb_backoff:new(BackoffOptions).
