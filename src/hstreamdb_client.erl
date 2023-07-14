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

-module(hstreamdb_client).

-define(GRPC_TIMEOUT, 5000).

-include("hstreamdb.hrl").

-export([
    start/2,
    stop/1,

    echo/1,
    create_stream/5,
    delete_stream/2,
    delete_stream/4,

    lookup_shard/2,
    list_shards/2,

    append/2,
    append/3,

    connect/3,
    connect/4
]).

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

start(Name, Options) when is_list(Options) ->
    start(Name, maps:from_list(Options));
start(Name, Options) ->
    ServerURL = maps:get(url, Options),
    RPCOptions = maps:get(rpc_options, Options),
    HostMapping = maps:get(host_mapping, Options, #{}),
    ChannelName = to_channel_name(Name),
    GRPCTimeout = maps:get(grpc_timeout, Options, ?GRPC_TIMEOUT),
    case validate_url_and_opts(ServerURL, maps:get(gun_opts, RPCOptions, #{})) of
        {ok, ServerURLMap, GunOpts} ->
            Client =
                #{
                    channel => ChannelName,
                    url_map => ServerURLMap,
                    rpc_options => RPCOptions#{gun_opts => GunOpts},
                    host_mapping => HostMapping,
                    grpc_timeout => GRPCTimeout
                },
            case start_channel(ChannelName, ServerURLMap, RPCOptions) of
                ok ->
                    {ok, Client};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

connect(Client, Host, Port) ->
    connect(Client, Host, Port, #{}).

connect(
    #{url_map := ServerURLMap, rpc_options := RPCOptions} = Client, Host0, Port, RPCOptionsOverrides
) ->
    Host1 = map_host(Client, Host0),
    NewChannelName = new_channel_name(Client, Host1, Port),
    NewUrlMap = maps:merge(ServerURLMap, #{
        host => Host1,
        port => Port
    }),
    NewRPCOptions = maps:merge(RPCOptions, RPCOptionsOverrides),
    case start_channel(NewChannelName, NewUrlMap, NewRPCOptions) of
        ok ->
            {ok, Client#{
                channel := NewChannelName, url_map := NewUrlMap, rpc_options := NewRPCOptions
            }};
        {error, Reason} ->
            {error, Reason}
    end.

stop(#{channel := Channel}) ->
    grpc_client_sup:stop_channel_pool(Channel);
stop(Name) ->
    Channel = to_channel_name(Name),
    grpc_client_sup:stop_channel_pool(Channel).

echo(Client) ->
    do_echo(Client).

create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount) ->
    do_create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount).

delete_stream(Client, Name) ->
    delete_stream(Client, Name, true, true).

delete_stream(Client, Name, IgnoreNonExist, Force) ->
    do_delete_stream(Client, Name, IgnoreNonExist, Force).

lookup_shard(Client, ShardId) ->
    do_lookup_shard(Client, ShardId).

list_shards(Client, StreamName) ->
    do_list_shards(Client, StreamName).

append(Client, Req) ->
    append(Client, Req, #{}).

append(Client, Req, Options) ->
    do_append(Client, Req, Options).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

new_channel_name(#{channel := ChannelName}, Host, Port) ->
    lists:concat([
        ChannelName,
        "_",
        to_channel_name(Host),
        "_",
        to_channel_name(Port),
        "_",
        to_channel_name(erlang:unique_integer([positive]))
    ]).

start_channel(ChannelName, URLMap, RPCOptions) ->
    URL = uri_string:recompose(URLMap),
    case grpc_client_sup:create_channel_pool(ChannelName, URL, RPCOptions) of
        {ok, _, _} ->
            ok;
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

validate_url_and_opts(URL, GunOpts) when is_binary(URL) ->
    validate_url_and_opts(list_to_binary(URL), GunOpts);
validate_url_and_opts(URL, GunOpts) when is_list(URL) ->
    case uri_string:parse(URL) of
        {error, What, Term} ->
            {error, {invalid_url, What, Term}};
        URIMap when is_map(URIMap) ->
            validate_scheme_and_opts(set_default_port(URIMap), GunOpts)
    end.

set_default_port(#{port := _Port} = URIMap) ->
    URIMap;
set_default_port(URIMap) ->
    URIMap#{port => ?DEFAULT_HSTREAMDB_PORT}.

validate_scheme_and_opts(#{scheme := "hstreams"} = URIMap, GunOpts) ->
    validate_scheme_and_opts(URIMap#{scheme := "https"}, GunOpts);
validate_scheme_and_opts(#{scheme := "hstream"} = URIMap, GunOpts) ->
    validate_scheme_and_opts(URIMap#{scheme := "http"}, GunOpts);
validate_scheme_and_opts(#{scheme := "https"}, #{transport := tcp}) ->
    {error, {https_invalid_transport, tcp}};
validate_scheme_and_opts(#{scheme := "https"} = URIMap, GunOpts) ->
    {ok, URIMap, GunOpts#{transport => tls}};
validate_scheme_and_opts(#{scheme := "http"}, #{transport := tls}) ->
    {error, {http_invalid_transport, tls}};
validate_scheme_and_opts(#{scheme := "http"} = URIMap, GunOpts) ->
    {ok, URIMap, GunOpts#{transport => tcp}};
validate_scheme_and_opts(_URIMap, _GunOpts) ->
    {error, unknown_scheme}.

map_host(#{host_mapping := HostMapping} = _Client, Host) ->
    host_to_string(maps:get(Host, HostMapping, Host)).

do_echo(#{channel := Channel, grpc_timeout := Timeout}) ->
    case ?HSTREAMDB_GEN_CLIENT:echo(#{}, #{channel => Channel, timeout => Timeout}) of
        {ok, #{msg := _}, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_create_stream(
    #{channel := Channel, grpc_timeout := Timeout},
    Name,
    ReplFactor,
    BacklogDuration,
    ShardCount
) ->
    Req = #{
        streamName => Name,
        replicationFactor => ReplFactor,
        backlogDuration => BacklogDuration,
        shardCount => ShardCount
    },
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:create_stream(Req, Options) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_delete_stream(
    #{channel := Channel, grpc_timeout := Timeout},
    Name,
    IgnoreNonExist,
    Force
) ->
    Req = #{
        streamName => Name,
        ignoreNonExist => IgnoreNonExist,
        force => Force
    },
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:delete_stream(Req, Options) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_lookup_shard(#{channel := Channel, grpc_timeout := Timeout}, ShardId) ->
    Req = #{shardId => ShardId},
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:lookup_shard(Req, Options) of
        {ok, #{serverNode := #{host := Host, port := Port}}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

do_list_shards(#{channel := Channel, grpc_timeout := Timeout}, StreamName) ->
    Req = #{streamName => StreamName},
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:list_shards(Req, Options) of
        {ok, #{shards := Shards}, _} ->
            logger:info("fetched shards for stream ~p: ~p~n", [StreamName, Shards]),
            {ok, Shards};
        {error, _} = Error ->
            Error
    end.

do_append(
    #{channel := Channel, grpc_timeout := Timeout},
    #{
        stream_name := StreamName,
        records := Records,
        shard_id := ShardId,
        compression_type := CompressionType
    },
    OptionOverrides
) ->
    case encode_records(Records, CompressionType) of
        {ok, NRecords} ->
            BatchedRecord = #{
                payload => NRecords,
                batchSize => length(Records),
                compressionType => compression_type_to_enum(CompressionType)
            },
            NReq = #{streamName => StreamName, shardId => ShardId, records => BatchedRecord},
            Options = maps:merge(#{channel => Channel, timeout => Timeout}, OptionOverrides),
            case timer:tc(fun() -> ?HSTREAMDB_GEN_CLIENT:append(NReq, Options) end) of
                {Time, {ok, Resp, _MetaData}} ->
                    logger:info("flush_request[~p, ~p], pid=~p, SUCCESS, ~p records in ~p ms~n", [
                        Channel, ShardId, self(), length(Records), Time div 1000
                    ]),
                    {ok, Resp};
                {Time, {error, R}} ->
                    logger:error(
                        "flush_request[~p, ~p], pid=~p, timeout=~p, ERROR: ~p, in ~p ms~n", [
                            Channel, ShardId, self(), Timeout, R, Time div 1000
                        ]
                    ),
                    {error, R}
            end;
        {error, R} ->
            {error, R}
    end.

host_to_string(Host) when is_list(Host) ->
    Host;
host_to_string(Host) when is_binary(Host) ->
    unicode:characters_to_list(Host).

to_channel_name(Integer) when is_integer(Integer) ->
    integer_to_list(Integer);
to_channel_name(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_channel_name(Binary) when is_binary(Binary) ->
    unicode:characters_to_list(Binary);
to_channel_name(List) when is_list(List) ->
    List.

encode_records(Records, CompressionType) ->
    case safe_encode_msg(Records) of
        {ok, Payload} ->
            case CompressionType of
                none -> {ok, Payload};
                gzip -> gzip(Payload);
                zstd -> zstd(Payload)
            end;
        {error, _} = Error ->
            Error
    end.

compression_type_to_enum(CompressionType) ->
    case CompressionType of
        none -> 0;
        gzip -> 1;
        zstd -> 2
    end.

safe_encode_msg(Records) ->
    try
        Payload = hstreamdb_api:encode_msg(
            #{records => Records},
            batch_h_stream_records,
            []
        ),
        {ok, Payload}
    catch
        error:Reason -> {error, {encode_msg, Reason}}
    end.

gzip(Payload) ->
    try
        {ok, zlib:gzip(Payload)}
    catch
        error:Reason -> {error, {gzip, Reason}}
    end.

zstd(Payload) ->
    case ezstd:compress(Payload) of
        {error, R} -> {error, {zstd, R}};
        R -> {ok, R}
    end.
