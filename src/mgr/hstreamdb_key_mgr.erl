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
    create/1,
    choose_shard/2,
    update_shards/2,
    shard_ids/1,
    set_shards/2
]).

%% For benchmarks/tests
-export([
    index_shards/1
]).

-export_type([t/0]).

-type t() :: #{
    shards := compiled_shards() | undefined,
    stream := hstreamdb:stream()
}.

-type compiled_shards() :: tuple().
-type shard_id() :: integer().

-type shard_info() :: #{
    isActive := _,
    startHashRangeKey := _,
    endHashRangeKey := _,
    shardId := _
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec create(hstreamdb:stream()) -> t().
create(StreamName) ->
    #{
        stream => StreamName,
        shards => undefined
    }.

-spec choose_shard(t(), binary()) -> not_found | {ok, shard_id()}.
choose_shard(#{shards := Shards}, PartitioningKey) ->
    find_shard(Shards, PartitioningKey).

-spec update_shards(hstreamdb:client(), t()) ->
    {ok, t()} | {error, term()}.
update_shards(Client, #{stream := StreamName} = KeyM) ->
    case list_shards(Client, StreamName) of
        {ok, Shards} ->
            NewKeyM = set_shards(KeyM, Shards),
            {ok, NewKeyM};
        {error, _} = Error ->
            Error
    end.

-spec set_shards(t(), [shard_info()]) -> t().
set_shards(KeyM, NewShards) ->
    KeyM#{shards := index_shards(NewShards)}.

-spec shard_ids(t()) -> not_initialized | {ok, [shard_id()]}.
shard_ids(#{
    shards := undefined
}) ->
    not_initialized;
shard_ids(#{
    shards := Shards
}) ->
    {ok, [ShardId || {_, _, ShardId} <- tuple_to_list(Shards)]}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

find_shard(Shards, PartitioningKey) ->
    <<IntHash:128/big-unsigned-integer>> = crypto:hash(md5, PartitioningKey),
    bsearch(Shards, IntHash, 1, tuple_size(Shards)).

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
