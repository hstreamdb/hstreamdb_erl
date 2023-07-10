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

-export([
    start/1,
    stop/1
]).

-export([
    random_channel_name/1,
    channel_name/1,
    start_channel/3,
    lookup_channel/2,
    bad_channel/2
]).

-include("hstreamdb.hrl").

start(Options) ->
    case proplists:get_value(client, Options, undefined) of
        #{
            channel := Channel,
            rpc_options := RPCOptions,
            host_mapping := HostMapping,
            grpc_timeout := GRPCTimeout,
            url_map := ServerURLMap
        } ->
            Stream = proplists:get_value(stream, Options, undefined),
            Stream == undefined andalso erlang:error({bad_options, no_stream_name}),
            #{
                channel => Channel,
                channels_by_shard => #{},
                host_mapping => HostMapping,
                grpc_timeout => GRPCTimeout,
                stream => Stream,
                rpc_options => RPCOptions,
                url_map => ServerURLMap
            };
        C ->
            erlang:error({bad_options, {bad_client, C}})
    end.

stop(#{channels_by_shard := Channels}) ->
    _ = [grpc_client_sup:stop_channel_pool(Channel) || Channel <- maps:values(Channels)],
    ok.

lookup_channel(
    ShardId,
    ChannelM = #{
        channels_by_shard := Channels,
        host_mapping := HostMapping,
        rpc_options := RPCOptions0,
        url_map := #{scheme := UrlScheme}
    }
) ->
    case maps:get(ShardId, Channels, undefined) of
        undefined ->
            case lookup_shard(ShardId, ChannelM) of
                {ok, {Host, Port}} ->
                    %% Producer need only one channel. Because it is a sync call.
                    ChannelUrlMap = #{
                        scheme => UrlScheme,
                        host => maybe_map_host(HostMapping, Host),
                        port => Port,
                        path => ""
                    },
                    logger:info("ServerURL for new channel: ~p~n", [ChannelUrlMap]),
                    RPCOptions = RPCOptions0#{pool_size => 1},
                    case start_channel(random_channel_name(Host), ChannelUrlMap, RPCOptions) of
                        {ok, Channel} ->
                            case echo(Channel, ChannelM) of
                                ok ->
                                    {ok, Channel, ChannelM#{
                                        channels_by_shard => Channels#{ShardId => Channel}
                                    }};
                                {error, Reason} ->
                                    ok = stop_channel(Channel),
                                    logger:info("Echo failed for new channel=~p: ~p~n", [
                                        Channel, Reason
                                    ]),
                                    {error, Reason}
                            end;
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end;
        Channel ->
            {ok, Channel, ChannelM}
    end.

echo(Channel, #{grpc_timeout := Timeout}) ->
    case ?HSTREAMDB_CLIENT:echo(#{}, #{channel => Channel, timeout => Timeout}) of
        {ok, #{msg := _}, _} ->
            ok;
        {error, _} = Error ->
            Error
    end.

lookup_shard(ShardId, #{channel := Channel, grpc_timeout := Timeout}) ->
    Req = #{shardId => ShardId},
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_CLIENT:lookup_shard(Req, Options) of
        {ok, #{serverNode := #{host := Host, port := Port}}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

bad_channel(BadChannel, ChannelM = #{channels_by_shard := ChannelsByShard}) ->
    ok = stop_channel(BadChannel),
    BadShards = [ShardId || {ShardId, Ch} <- maps:to_list(ChannelsByShard), Ch =:= BadChannel],
    logger:info("BadChannel: ~p, BadShards: ~p~n", [BadChannel, BadShards]),
    NChannelM = ChannelM#{channels_by_shard => maps:without(BadShards, ChannelsByShard)},
    NChannelM.

stop_channel(undefined) ->
    ok;
stop_channel(Channel) ->
    try
        Res = grpc_client_sup:stop_channel_pool(Channel),
        logger:info("hstreamdb_channel_mgr stop channel[~p]: ~p", [Channel, Res])
    catch
        Class:Error ->
            logger:error("hstreamdb_channel_mgr stop channel[~p]: ~p", [Channel, {Class, Error}]),
            ok
    end.

random_channel_name(Name) ->
    lists:concat([to_channel_name(Name), erlang:unique_integer()]).

channel_name(Name) ->
    to_channel_name(Name).

maybe_map_host(HostMapping, Host) ->
    host_to_string(maps:get(Host, HostMapping, Host)).

start_channel(ChannelName, URLMap, RPCOptions) ->
    URL = uri_string:recompose(URLMap),
    case grpc_client_sup:create_channel_pool(ChannelName, URL, RPCOptions) of
        {ok, _, _} ->
            {ok, ChannelName};
        {ok, _} ->
            {ok, ChannelName};
        {error, Reason} ->
            {error, Reason}
    end.

to_channel_name(Integer) when is_integer(Integer) ->
    integer_to_list(Integer);
to_channel_name(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_channel_name(Binary) when is_binary(Binary) ->
    unicode:characters_to_list(Binary);
to_channel_name(List) when is_list(List) ->
    List.

host_to_string(Host) when is_list(Host) ->
    Host;
host_to_string(Host) when is_binary(Host) ->
    unicode:characters_to_list(Host).
