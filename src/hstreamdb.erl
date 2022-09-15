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

-export([ echo/1
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
    Client =
        #{
            channel => ChannelName,
            url => ServerURL,
            rpc_options => RPCOptions,
            url_prefix => url_prefix(ServerURL),
            host_mapping => HostMapping,
            grpc_timeout => GRPCTimeout
        },
    case hstreamdb_channel_mgr:start_channel(ChannelName, ServerURL, RPCOptions) of
        {ok,_} ->
            {ok, Client};
        {error, Reason} ->
            {error, Reason}
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

to_record(OrderingKey, PayloadType, Payload) ->
    {OrderingKey, #{
        header => #{
            flag => case PayloadType of
                        json -> 0;
                        raw -> 1
                    end,
            key => OrderingKey
            },
        payload => Payload
    }}.

echo(Client) ->
    do_echo(Client).

append(Producer, OrderingKey, PayloadType, Payload) ->
    Record = to_record(OrderingKey, PayloadType, Payload),
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
    "https://";
url_prefix(<<"http://", _/binary>>) ->
    "http://".

do_echo(#{channel := Channel, grpc_timeout := Timeout}) ->
    case ?HSTREAMDB_CLIENT:echo(#{}, #{channel => Channel, timeout => Timeout}) of
        {ok, #{msg := _}, _} ->
            {ok, echo};
        {error, R} ->
            {error, R}
    end.
