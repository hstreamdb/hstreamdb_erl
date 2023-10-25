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

%% @doc `hstreamdb_key_mgr` wrapper with automatic shard update on timeout.

-module(hstreamdb_auto_key_mgr).

-export([
    start/2,
    start/3,
    stop/1,
    choose_shard/2,
    set_shards/2
]).

-export_type([t/0]).

-define(DEFAULT_SHARD_UPDATE_INTERVAL, 3000000).

-type t() :: #{
    client := hstreamdb:client(),
    shard_update_deadline := integer(),
    shard_update_interval := non_neg_integer(),
    key_manager := hstreamdb_key_mgr:t()
}.

-type options() :: #{
    shard_update_interval => pos_integer()
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start(hstreamdb:client(), hstreamdb:stream()) -> t().
start(Client, StreamName) ->
    start(Client, StreamName, #{}).

-spec start(hstreamdb:client(), hstreamdb:stream(), options()) -> t().
start(Client, StreamName, Options) ->
    #{
        client => Client,
        shards => undefined,
        shard_update_interval => maps:get(
            shard_update_interval,
            Options,
            ?DEFAULT_SHARD_UPDATE_INTERVAL
        ),
        shard_update_deadline => erlang:monotonic_time(millisecond),
        key_manager => hstreamdb_key_mgr:create(StreamName)
    }.

-spec stop(t()) -> ok.
stop(#{}) ->
    ok.

-spec choose_shard(t(), binary()) -> {ok, integer(), t()} | {error, term()}.
choose_shard(Mgr0, PartitioningKey) ->
    case update_shards(Mgr0) of
        {ok, #{key_manager := KeyManager} = Mgr1} ->
            case hstreamdb_key_mgr:choose_shard(KeyManager, PartitioningKey) of
                not_found ->
                    {error, {cannot_find_shard, PartitioningKey}};
                {ok, ShardId} ->
                    {ok, ShardId, Mgr1}
            end;
        {error, _} = Error ->
            Error
    end.

-spec set_shards(t(), [hstreamdb_key_mgr:shard_info()]) -> t().
set_shards(
    #{key_manager := KeyManager, shard_update_interval := ShardUpdateInterval} = Mgr, Shards
) ->
    Mgr#{
        key_manager := hstreamdb_key_mgr:set_shards(KeyManager, Shards),
        shard_update_deadline => erlang:monotonic_time(millisecond) + ShardUpdateInterval
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

update_shards(
    #{
        key_manager := KeyManager0,
        client := Client,
        shard_update_deadline := Deadline,
        shard_update_interval := ShardUpdateInterval
    } = Mgr
) ->
    case erlang:monotonic_time(millisecond) >= Deadline of
        true ->
            case hstreamdb_key_mgr:update_shards(Client, KeyManager0) of
                {ok, KeyManager1} ->
                    {ok, Mgr#{
                        key_manager := KeyManager1,
                        shard_update_deadline => erlang:monotonic_time(millisecond) +
                            ShardUpdateInterval
                    }};
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, Mgr}
    end.
