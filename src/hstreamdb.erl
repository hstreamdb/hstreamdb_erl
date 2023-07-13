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

-module(hstreamdb).

-export([
    start_client/2,
    stop_client/1,
    start_producer/3,
    stop_producer/1,
    start_consumer/3,
    stop_consumer/1
]).

-export([
    echo/1,
    create_stream/5,
    delete_stream/2,
    delete_stream/4
]).

-export([
    to_record/3,
    append/2,
    append/4,
    flush/1,
    append_flush/2
]).

-define(GRPC_TIMEOUT, 5000).

-include("hstreamdb.hrl").

start_client(Name, Options) when is_list(Options) ->
    start_client(Name, maps:from_list(Options));
start_client(Name, Options) ->
    ServerURL = maps:get(url, Options),
    RPCOptions = maps:get(rpc_options, Options),
    HostMapping = maps:get(host_mapping, Options, #{}),
    ChannelName = hstreamdb_channel_mgr:channel_name(Name),
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
            case hstreamdb_channel_mgr:start_channel(ChannelName, ServerURLMap, RPCOptions) of
                {ok, _} ->
                    {ok, Client};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

stop_client(#{channel := Channel}) ->
    grpc_client_sup:stop_channel_pool(Channel);
stop_client(Name) ->
    Channel = hstreamdb_channel_mgr:channel_name(Name),
    grpc_client_sup:stop_channel_pool(Channel).

start_producer(Client, Producer, ProducerOptions) ->
    hstreamdb_producer:start(Producer, [{client, Client} | ProducerOptions]).

stop_producer(Producer) ->
    hstreamdb_producer:stop(Producer).

start_consumer(_Client, Consumer, _ConsumerOptions) ->
    {ok, Consumer}.

stop_consumer(_) ->
    ok.

to_record(PartitioningKey, PayloadType, Payload) ->
    {PartitioningKey, #{
        header => #{
            flag =>
                case PayloadType of
                    json -> 0;
                    raw -> 1
                end,
            key => PartitioningKey
        },
        payload => Payload
    }}.

echo(Client) ->
    do_echo(Client).

create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount) ->
    do_create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount).

delete_stream(Client, Name) ->
    delete_stream(Client, Name, true, true).

delete_stream(Client, Name, IgnoreNonExist, Force) ->
    do_delete_stream(Client, Name, IgnoreNonExist, Force).

append(Producer, PartitioningKey, PayloadType, Payload) ->
    Record = to_record(PartitioningKey, PayloadType, Payload),
    append(Producer, Record).

append(Producer, Record) ->
    hstreamdb_producer:append(Producer, Record).

flush(Producer) ->
    hstreamdb_producer:flush(Producer).

append_flush(Producer, Data) ->
    hstreamdb_producer:append_flush(Producer, Data).

%% -------------------------------------------------------------------------------------------------
%% internal

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

do_echo(#{channel := Channel, grpc_timeout := Timeout}) ->
    case ?HSTREAMDB_CLIENT:echo(#{}, #{channel => Channel, timeout => Timeout}) of
        {ok, #{msg := _}, _} ->
            {ok, echo};
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
    case ?HSTREAMDB_CLIENT:create_stream(Req, Options) of
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
    case ?HSTREAMDB_CLIENT:delete_stream(Req, Options) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.
