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

-define(DEFAULT_SHARD_UPDATE_INTERVAL, 3000000).

start(Client, StreamName) ->
    start(Client, StreamName, []).

start(Client, StreamName, Options) ->
    #{
        client => Client,
        stream => StreamName,
        shards => undefined,
        shard_update_interval => proplists:get_value(
            shard_update_interval,
            Options,
            ?DEFAULT_SHARD_UPDATE_INTERVAL
        ),
        shard_update_deadline => erlang:monotonic_time(millisecond)
    }.

stop(#{}) ->
    ok.

update_shards(
    #{
        shard_update_deadline := Deadline,
        shard_update_interval := ShardUpateInterval,
        stream := StreamName,
        client := Client
    } = KeyM
) ->
    Now = erlang:monotonic_time(millisecond),
    case Deadline =< Now of
        true ->
            KeyM#{
                shards := list_shards(Client, StreamName),
                shard_update_deadline := Now + ShardUpateInterval
            };
        false ->
            KeyM
    end.

list_shards(Client, StreamName) ->
    case hstreamdb_client:list_shards(Client, StreamName) of
        {ok, Shards} ->
            logger:info("fetched shards for stream ~p: ~p~n", [StreamName, Shards]),
            Shards;
        {error, Error} ->
            erlang:error({cannot_list_shards, {StreamName, Error}})
    end.

choose_shard(PartitioningKey, KeyMgr0) ->
    KeyMgr1 = update_shards(KeyMgr0),
    #{shards := Shards} = KeyMgr1,
    <<IntHash:128/big-unsigned-integer>> = crypto:hash(md5, PartitioningKey),
    [#{shardId := ShardId}] =
        lists:filter(
            fun(
                #{
                    startHashRangeKey := SHRK,
                    endHashRangeKey := EHRK
                }
            ) ->
                IntHash >= binary_to_integer(SHRK) andalso
                    IntHash =< binary_to_integer(EHRK)
            end,
            Shards
        ),
    {ShardId, KeyMgr1}.
