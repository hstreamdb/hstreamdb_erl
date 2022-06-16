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

-export([ start/1
        , stop/1
        ]).

-export([ random_channel_name/1
        , channel_name/1 
        , start_channel/3
        , lookup_channel/2
        , bad_channel/2
        ]).

start(Options) ->
    case proplists:get_value(client, Options, undefined) of
        #{channel := Channel,
         rpc_options := RPCOptions,
         url_prefix := UrlPrefix} ->
            Stream = proplists:get_value(stream, Options, undefined),
            Stream == undefined andalso erlang:error({bad_options, no_stream_name}),
            #{
                channel => Channel,
                channels => #{},
                stream => Stream,
                rpc_options => RPCOptions,
                url_prefix => UrlPrefix
            };
        C ->
            erlang:error({bad_options, {bad_client, C}})
    end.

stop(#{channels := Channels}) ->
    _ = [grpc_client_sup:stop_channel_pool(Channel) || Channel <- maps:values(Channels)],
    ok.

lookup_channel(OrderingKey, ChannelM = #{channels := Channels,
                                         stream := Stream,
                                         rpc_options := RPCOptions,
                                         url_prefix := UrlPrefix}) ->
    case maps:get(OrderingKey, Channels, undefined) of
        undefined ->
            case lookup_stream(OrderingKey, Stream, ChannelM) of
                {ok, #{host := Host, port := Port}} ->
                    ServerURL = lists:concat(io_lib:format("~s~s~s~p", [UrlPrefix, Host, ":", Port])),
                    case start_channel(random_channel_name(OrderingKey), ServerURL, RPCOptions) of
                        {ok, Channel} ->
                            {ok, Channel, ChannelM#{channels => Channels#{OrderingKey => Channel}}};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        Channel ->
            {ok, Channel}
    end.

bad_channel(OrderingKey, ChannelM = #{channels := Channels}) ->
    ChannelM#{channels => maps:remove(OrderingKey, Channels)}.

random_channel_name(Name) ->
    lists:concat([Name,  erlang:unique_integer()]). 
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

lookup_stream(OrderingKey, Stream, #{channel := Channel}) ->
    Req = #{'orderingKey' => OrderingKey, 'streamName' => Stream},
    Options = #{channel => Channel},
    case hstreamdb_client:lookup_stream(Req, Options) of
        {ok, Resp, _} ->
            {ok, maps:get('serverNode', Resp)};
        {error, Error} ->
            {error, Error}
    end.
