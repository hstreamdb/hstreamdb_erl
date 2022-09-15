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

-behaviour(gen_server).

-export([init/1]).

-export([ start/1
        , stop/1
        ]).

-export([ channel_name/1
        , start_channel/3
        , mark_as_bad_channel/2
        ]).

-export([lookup_ordering_key/2]).

-export([terminate/2, handle_cast/2, handle_call/3]).

-record(state, {channel, channels, pool_size, stream, url_prefix, rpc_options, grpc_pool_size, url_map}).

lookup_ordering_key(ServerRef, OrderingKey) ->
    gen_server:call(ServerRef, {get, OrderingKey}).

mark_as_bad_channel(ServerRef, OrderingKey) ->
    gen_server:call(ServerRef, {bad_channel, OrderingKey}).

init(Options) ->
    GrpcPoolSize = proplists:get_value(grpc_pool_size, Options, 8),
    PoolSize = proplists:get_value(pool_size, Options),
    case proplists:get_value(client, Options, undefined) of
        #{channel := Channel,
         rpc_options := RPCOptions,
         url_prefix := UrlPrefix} ->
            Stream = proplists:get_value(stream, Options, undefined),
            case Stream == undefined of
                true  -> {error, {bad_options, no_stream_name}};
                false -> {ok, #state{
                channel = Channel,
                channels = #{},
                stream = Stream,
                rpc_options = RPCOptions,
                url_prefix = UrlPrefix,
                grpc_pool_size = GrpcPoolSize,
                pool_size = PoolSize,
                url_map = #{}
            }} end;
        C ->
            {error, {bad_options, {bad_client, C}}}
    end.

start(Options) ->
    gen_server:start(?MODULE, Options, []).

terminate(_, #state{channels = Channels}) ->
    _ = [grpc_client_sup:stop_channel_pool(Channel) || Channel <- maps:values(Channels)],
    ok.

stop(ServerRef) ->
    gen_server:stop(ServerRef).

handle_cast(_, State) ->
    {noreply, State}.

handle_call({get, OrderingKey}, _From, State) ->
    case lookup_channel(OrderingKey, State) of
        {error, Error} ->
            NState = bad_channel(OrderingKey, State),
            {reply, {error, Error}, NState};
        {ok, Channel} ->
            {reply, {ok, Channel}, State};
        {ok, Channel, NState} ->
            {reply, {ok, Channel}, NState}
    end;
handle_call({bad_channel, OrderingKey}, _From, State) ->
    NState = bad_channel(OrderingKey, State),
    {reply, ok, NState}.

lookup_channel(OrderingKey, ChannelM = #state{channels = Channels,
                                         stream = Stream,
                                         rpc_options = RPCOptions0,
                                         url_prefix = UrlPrefix,
                                         pool_size = PoolSize,
                                         grpc_pool_size = GrpcPoolSize,
                                         url_map = UrlMap
                                        }) ->
    RPCOptions = RPCOptions0#{pool_size => PoolSize * GrpcPoolSize},
    case maps:get(OrderingKey, UrlMap, undefined) of
        undefined ->
            case lookup_stream(OrderingKey, Stream, ChannelM) of
                {ok, #{host := Host, port := Port}} ->
                    ServerUrl = lists:concat(io_lib:format("~s~s~s~p", [UrlPrefix, Host, ":", Port])),
                    case start_channel(random_channel_name(OrderingKey), ServerUrl, RPCOptions) of
                        {ok, Channel} ->
                            {ok, Channel, ChannelM#state{url_map = UrlMap#{OrderingKey => ServerUrl}, channels = Channels#{ServerUrl => Channel}}};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        ServerUrl ->
            case maps:get(ServerUrl, UrlMap, undefined) of
                undefined ->
                    case start_channel(random_channel_name(OrderingKey), ServerUrl, RPCOptions) of
                        {ok, Channel} ->
                            {ok, Channel, ChannelM#state{channels = Channels#{ServerUrl => Channel}}};
                        {error, Reason} ->
                            {error, Reason};
                Channel -> {ok, Channel}
                    end
            end
    end.

bad_channel(OrderingKey, ChannelM = #state{channels = Channels, url_map = UrlMap}) ->
    case maps:get(OrderingKey, UrlMap, Channels) of
        undefined ->
            ChannelM#state{url_map = maps:remove(OrderingKey, UrlMap)};
        ServerUrl ->
            ok = stop_channel(maps:get(ServerUrl, Channels, undefined)),
            ChannelM#state{url_map = maps:remove(OrderingKey, UrlMap), channels = maps:remove(ServerUrl, Channels)}
    end.

stop_channel(undefined) -> ok;
stop_channel(Channel) ->
    try grpc_client_sup:stop_channel_pool(Channel)
    catch _:_ ->
        ok
    end.

random_channel_name(Name) ->
    lists:concat([Name,  erlang:unique_integer()]).
channel_name(Name) ->
    lists:concat([Name]).

start_channel(ChannelName, ServerUrl, RPCOptions) ->
    case grpc_client_sup:create_channel_pool(ChannelName, ServerUrl, RPCOptions) of
        {ok, _, _} ->
            {ok, ChannelName};
        {ok, _} ->
            {ok, ChannelName};
        {error, Reason} ->
            {error, Reason}
    end.

lookup_stream(OrderingKey, Stream, #state{channel = Channel}) ->
    Req = #{'orderingKey' => OrderingKey, 'streamName' => Stream},
    Options = #{channel => Channel},
    case hstreamdb_client:lookup_stream(Req, Options) of
        {ok, Resp, _} ->
            {ok, maps:get('serverNode', Resp)};
        {error, Error} ->
            {error, Error}
    end.
