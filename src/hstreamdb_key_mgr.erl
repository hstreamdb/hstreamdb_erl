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
    start/1,
    stop/1
]).

-export([choose_shard/2]).

-include("hstreamdb.hrl").

-define(DEFAULT_SHARD_UPDATE_INTERVAL, 3000000).

start(Options) ->
    case proplists:get_value(client, Options, undefined) of
        #{
            channel := Channel,
            grpc_timeout := GRPCTimeout
        } ->
            Stream = proplists:get_value(stream, Options, undefined),
            Stream == undefined andalso erlang:error({bad_options, no_stream_name}),
            #{
                channel => Channel,
                stream => Stream,
                grpc_timeout => GRPCTimeout,
                shards => undefined,
                shard_update_interval => proplists:get_value(
                    shard_update_interval,
                    Options,
                    ?DEFAULT_SHARD_UPDATE_INTERVAL
                ),
                shard_update_deadline => erlang:monotonic_time(millisecond)
            };
        C ->
            erlang:error({bad_options, {bad_client, C}})
    end.

stop(#{}) ->
    ok.

update_shards(
    #{
        shard_update_deadline := Deadline,
        shard_update_interval := ShardUpateInterval
    } = ChannelM
) ->
    Now = erlang:monotonic_time(millisecond),
    case Deadline =< Now of
        true ->
            #{stream := StreamName, channel := Channel, grpc_timeout := Timeout} = ChannelM,
            ChannelM#{
                shards := list_shards(StreamName, Channel, Timeout),
                shard_update_deadline := Now + ShardUpateInterval
            };
        false ->
            ChannelM
    end.

list_shards(Stream, Channel, GRPCTimeout) ->
    Req = #{'streamName' => Stream},
    Options = #{channel => Channel, timeout => GRPCTimeout},
    case ?HSTREAMDB_CLIENT:list_shards(Req, Options) of
        {ok, #{shards := Shards}, _} ->
            logger:info("fetched shards for stream ~p: ~p~n", [Stream, Shards]),
            Shards;
        {error, _} = Error ->
            erlang:error({cannot_list_shards, {Stream, Error}})
    end.

choose_shard(PartitioningKey, ChannelM0) ->
    ChannelM1 = update_shards(ChannelM0),
    #{shards := Shards} = ChannelM1,
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
    {ShardId, ChannelM1}.
