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
-module(hstreamdb_channel_mgr).

-define(GRPC_ECHO_TIMEOUT, 500).

-export([ start/1
        , stop/1
        ]).

-export([ random_channel_name/1
        , channel_name/1 
        , start_channel/3
        , lookup_channel/2
        , bad_channel/2
        ]).

-include("hstreamdb.hrl").

start(Options) ->
    case proplists:get_value(client, Options, undefined) of
        #{channel := Channel,
          rpc_options := RPCOptions,
          url_prefix := UrlPrefix} ->
            Stream = proplists:get_value(stream, Options, undefined),
            Stream == undefined andalso erlang:error({bad_options, no_stream_name}),
            #{
                channel => Channel,
                channels_by_node => #{},
                stream => Stream,
                rpc_options => RPCOptions,
                url_prefix => UrlPrefix
            };
        C ->
            erlang:error({bad_options, {bad_client, C}})
    end.

stop(#{channels_by_node := Channels}) ->
    _ = [grpc_client_sup:stop_channel_pool(Channel) || Channel <- maps:values(Channels)],
    ok.

lookup_channel(Node, ChannelM = #{ channels_by_node := ChannelsByNode,
                                   rpc_options := RPCOptions0,
                                   url_prefix := UrlPrefix
                                 }) ->
    case maps:get(Node, ChannelsByNode, undefined) of
        undefined ->
            {Host, Port} = Node,
            %% Producer need only one channel. Because it is sync call.
            ServerURL = lists:concat(io_lib:format("~s~s~s~p", [UrlPrefix, Host, ":", Port])),
            logger:info("ServerURL for new channel: ~p~n", [ServerURL]),
            RPCOptions = RPCOptions0#{pool_size => 1},
            case start_channel(random_channel_name(ServerURL), ServerURL, RPCOptions) of
                {ok, Channel} ->
                    case ?HSTREAMDB_CLIENT:echo(#{}, #{channel => Channel, timeout => ?GRPC_ECHO_TIMEOUT}) of
                        {ok, #{msg := _}, _} ->
                            {ok, Channel, ChannelM#{channels_by_node => ChannelsByNode#{Node => Channel}}};
                        {error, Reason} ->
                            ok = stop_channel(Channel),
                            logger:info("Echo failed for new channel=~p: ~p~n", [Channel, Reason]),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        Channel ->
            {ok, Channel, ChannelM}
    end.

bad_channel(BadChannel, ChannelM = #{channels_by_node := ChannelsByNode}) ->
    ok = stop_channel(BadChannel),
    BadNodes = [Node || {Node, Ch} <- maps:to_list(ChannelsByNode), Ch =:= BadChannel],
    logger:info("BadChannel: ~p, BadNodes: ~p~n", [BadChannel, BadNodes]),
    NChannelM = ChannelM#{channels_by_node => maps:without(BadNodes, ChannelsByNode)},
    NChannelM.

stop_channel(undefined) -> ok;
stop_channel(Channel) ->
    try 
        Res = grpc_client_sup:stop_channel_pool(Channel),
        logger:info("hstreamdb_channel_mgr stop channel[~p]: ~p", [Channel, Res])
    catch Class:Error ->
        logger:error("hstreamdb_channel_mgr stop channel[~p]: ~p", [Channel, {Class, Error}]),
        ok
    end.

random_channel_name(Name) ->
    lists:concat([Name, erlang:unique_integer()]). 
channel_name(Name) ->
    lists:concat([Name]).

start_channel(ChannelName, ServerURL, RPCOptions) ->
    case grpc_client_sup:create_channel_pool(ChannelName, ServerURL, RPCOptions) of
        {ok, _, _} ->
            {ok, ChannelName};
        {ok, _} ->
            {ok, ChannelName};
        {error, Reason} ->
            {error, Reason}
    end.

