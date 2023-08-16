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

-module(hstreamdb_key_mgr).

-export([
    start/2,
    start/3,
    stop/1
]).

-export([choose_shard/2]).

%% For benchmarks/tests
-export([
    find_shard/2,
    index_shards/1,
    update_shards/2
]).

-export_type([t/0]).

-define(DEFAULT_SHARD_UPDATE_INTERVAL, 3000000).

-type t() :: #{
    client := hstreamdb:client(),
    shard_update_deadline := integer(),
    shard_update_interval := non_neg_integer(),
    shards := compiled_shards() | undefined,
    stream := hstreamdb:stream()
}.

-type compiled_shards() :: tuple().

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
        stream => StreamName,
        shards => undefined,
        shard_update_interval => maps:get(
            shard_update_interval,
            Options,
            ?DEFAULT_SHARD_UPDATE_INTERVAL
        ),
        shard_update_deadline => erlang:monotonic_time(millisecond)
    }.

-spec stop(t()) -> ok.
stop(#{}) ->
    ok.

-spec choose_shard(t(), binary()) -> {ok, integer(), t()} | {error, term()}.
choose_shard(KeyMgr0, PartitioningKey) ->
    case update_shards(KeyMgr0) of
        {ok, KeyMgr1} ->
            #{shards := Shards} = KeyMgr1,
            case find_shard(Shards, PartitioningKey) of
                not_found ->
                    {error, {cannot_find_shard, PartitioningKey}};
                {ok, ShardId} ->
                    {ok, ShardId, KeyMgr1}
            end;
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

update_shards(
    #{
        shard_update_interval := ShardUpateInterval
    } = KeyM,
    NewShards
) ->
    KeyM#{
        shards := index_shards(NewShards),
        shard_update_deadline := erlang:monotonic_time(millisecond) + ShardUpateInterval
    }.

update_shards(
    #{
        shard_update_deadline := Deadline,
        stream := StreamName,
        client := Client
    } = KeyM
) ->
    Now = erlang:monotonic_time(millisecond),
    case Deadline =< Now of
        true ->
            case list_shards(Client, StreamName) of
                {ok, Shards} ->
                    NewKeyM = update_shards(KeyM, Shards),
                    {ok, NewKeyM};
                {error, _} = Error ->
                    Error
            end;
        false ->
            {ok, KeyM}
    end.

index_shards(Shards) ->
    Compiled = lists:filtermap(
        fun
            (
                #{
                    isActive := false
                }
            ) ->
                false;
            (
                #{
                    startHashRangeKey := StartBin,
                    endHashRangeKey := EndBin,
                    shardId := ShardId
                }
            ) ->
                {true, {binary_to_integer(StartBin), binary_to_integer(EndBin), ShardId}}
        end,
        Shards
    ),
    list_to_tuple(lists:sort(Compiled)).

find_shard(Shards, PartitioningKey) ->
    <<IntHash:128/big-unsigned-integer>> = crypto:hash(md5, PartitioningKey),
    bsearch(Shards, IntHash, 1, tuple_size(Shards)).

bsearch(_Shards, _IntHash, From, To) when From > To ->
    not_found;
bsearch(Shards, IntHash, From, To) ->
    Med = (From + To) div 2,
    case element(Med, Shards) of
        {Start, End, ShardId} when IntHash >= Start, IntHash =< End ->
            {ok, ShardId};
        {Start, _, _} when IntHash < Start ->
            bsearch(Shards, IntHash, From, Med - 1);
        {_, End, _} when IntHash > End ->
            bsearch(Shards, IntHash, Med + 1, To)
    end.

list_shards(Client, StreamName) ->
    case hstreamdb_client:list_shards(Client, StreamName) of
        {ok, Shards} ->
            logger:info("[hstreamdb] fetched shards for stream ~p: ~p~n", [StreamName, Shards]),
            {ok, Shards};
        {error, Error} ->
            {error, {cannot_list_shards, {StreamName, Error}}}
    end.
