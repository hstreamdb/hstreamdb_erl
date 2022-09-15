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

-module(hstreamdb_ordkey_mgr).

-export([ start/1
        , stop/1
        ]).

-export([ lookup_channel/2 ]).

-include("hstreamdb.hrl").

-define(TAB, ?MODULE).

start(Options) ->
    case proplists:get_value(client, Options, undefined) of
        #{channel := Channel,
         grpc_timeout := GRPCTimeout,
         host_mapping := HostMapping} ->
            Stream = proplists:get_value(stream, Options, undefined),
            Stream == undefined andalso erlang:error({bad_options, no_stream_name}),
            #{
                channel => Channel,
                stream => Stream,
                host_mapping => HostMapping,
                grpc_timeout => GRPCTimeout
            };
        C ->
            erlang:error({bad_options, {bad_client, C}})
    end.

stop(#{}) ->
    ok.

lookup_channel(OrderingKey, ChannelM = #{host_mapping := HostMapping,
                                         stream := Stream}) ->
    Key = {Stream, OrderingKey},
    case ets:lookup(?TAB, Key) of
        [] ->
            case lookup_shard(OrderingKey, Stream, ChannelM) of
               {ok, ShardId, #{host := Host, port := Port}} ->
                    RealHost = maps:get(Host, HostMapping, Host),
                    ShardNode = {ShardId, {RealHost, Port}},
                    true = ets:insert(?TAB, {Key, ShardNode}),
                    {ok, ShardNode};
                {error, Reason} ->
                    {error, Reason}
            end;
        [{Key, ShardNode}] ->
            {ok, ShardNode}
    end.

lookup_shard(OrderingKey, Stream, #{channel := Channel, grpc_timeout := GRPCTimeout}) ->
    Req = #{'streamName' => Stream},
    Options = #{channel => Channel, timeout => GRPCTimeout},
    <<IntHash:128/big-unsigned-integer>> = crypto:hash(md5, OrderingKey),
    case ?HSTREAMDB_CLIENT:list_shards(Req, Options) of
        {ok, #{shards := Shards}, _} ->
            [#{shardId := ShardId}] =
            lists:filter(
              fun(#{startHashRangeKey := SHRK,
                    endHashRangeKey := EHRK}) ->
                      IntHash >= binary_to_integer(SHRK) andalso
                      IntHash =< binary_to_integer(EHRK)
              end,
              Shards),
            case ?HSTREAMDB_CLIENT:lookup_shard(#{shardId => ShardId}, Options) of
                {ok, #{serverNode := ServerNode}, _} ->
                    {ok, ShardId, ServerNode};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            logger:error("list_shards for stream ~p error: ~p~n", [Stream, Error]),
            {error, Error}
    end.


