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
            case lookup_stream(OrderingKey, ChannelM) of
               {ok, #{host := Host, port := Port}} ->
                    RealHost = maps:get(Host, HostMapping, Host),
                    Node = {RealHost, Port},
                    true = ets:insert(?TAB, {Key, Node}),
                    {ok, Node};
                {error, Reason} ->
                    {error, Reason}
            end;
        [{Key, Node}] ->
            {ok, Node}
    end.

lookup_stream(OrderingKey,
              #{stream := Stream, channel := Channel, grpc_timeout := GRPCTimeout}) ->
    Req = #{'orderingKey' => OrderingKey, 'streamName' => Stream},
    Options = #{channel => Channel, timeout => GRPCTimeout},
    case hstreamdb_client:lookup_stream(Req, Options) of
        {ok, Resp, _} ->
            {ok, maps:get('serverNode', Resp)};
        {error, Error} ->
            {error, Error}
    end.



