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

-export([ start_client/2
        , stop_client/1
        , start_producer/3
        , stop_producer/1
        , start_consumer/3
        , stop_consumer/1
        ]).

-export([ echo/1,
          create_stream/5,
          delete_stream/2,
          delete_stream/4
        ]).

-export([ to_record/3
        , append/2
        , append/4
        , flush/1
        , append_flush/2
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
    case url_prefix(ServerURL) of
        {ok, Prefix} ->
            Client =
                #{
                    channel => ChannelName,
                    url => ServerURL,
                    rpc_options => RPCOptions,
                    url_prefix => Prefix,
                    host_mapping => HostMapping,
                    grpc_timeout => GRPCTimeout
                },
            case hstreamdb_channel_mgr:start_channel(ChannelName, ServerURL, RPCOptions) of
                {ok,_} ->
                    {ok, Client};
                {error, Reason} ->
                    {error, Reason}
            end;
        error ->
            {error, invalid_server_url}
    end.

stop_client(#{channel := Channel}) ->
    grpc_client_sup:stop_channel_pool(Channel);
stop_client(Name) ->
    Channel =  hstreamdb_channel_mgr:channel_name(Name),
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
            flag => case PayloadType of
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

url_prefix(URL) when is_list(URL) ->
    url_prefix(list_to_binary(URL));
url_prefix(<<"https://", _/binary>>) ->
    {ok, "https://"};
url_prefix(<<"http://", _/binary>>) ->
    {ok, "http://"};
url_prefix(_) ->
    error.

do_echo(#{channel := Channel, grpc_timeout := Timeout}) ->
    case ?HSTREAMDB_CLIENT:echo(#{}, #{channel => Channel, timeout => Timeout}) of
        {ok, #{msg := _}, _} ->
            {ok, echo};
        {error, R} ->
            {error, R}
    end.

do_create_stream(#{channel := Channel, grpc_timeout := Timeout},
                 Name, ReplFactor, BacklogDuration, ShardCount) ->
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

do_delete_stream(#{channel := Channel, grpc_timeout := Timeout},
                 Name, IgnoreNonExist, Force) ->
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
