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

-include("hstreamdb.hrl").

-export([
    start_producer/2,
    stop_producer/1,
    to_record/3,
    append/2,
    append/4,
    flush/1,
    append_flush/2,
    append_sync/2
]).

-export([
    start_client_manager/1,
    start_client_manager/2,
    stop_client_manager/1
]).

-export([
    start_key_manager/2,
    start_key_manager/3,
    stop_key_manager/1
]).

-export([
    read_single_shard_stream/2,
    read_single_shard_stream/3
]).

-export([
    start_reader/2,
    stop_reader/1,
    read_stream_key/3
]).

-export_type([
    stream/0,
    partitioning_key/0,
    client/0,
    shard_id/0,
    hrecord/0
]).

-type stream() :: binary() | string().
-type partitioning_key() :: binary() | string().
-type client() :: hstreamdb_client:t().
-type shard_id() :: integer().

-type hrecord_attributes() :: #{}.
-type hrecord_header() :: #{
    flag := 'RAW',
    key := partitioning_key(),
    attributes := hrecord_attributes()
}.
-type hrecord_payload() :: binary().
-type hrecord_id() :: #{
    batchId := integer(),
    batchIndex := integer(),
    shardId := shard_id()
}.
-type hrecord() :: #{
    header := hrecord_header(),
    payload := hrecord_payload(),
    recordId := hrecord_id()
}.

-type special_offset() :: {specialOffset, 0 | 1}.
-type timestamp_offset() :: {timestampOffset, #{timestampInMs => integer()}}.
-type record_offset() :: {recordOffset, hrecord_id()}.

-type offset() :: #{offset => special_offset() | timestamp_offset() | record_offset()}.

-type limits() :: #{from => offset(), until => offset(), max_read_batches => non_neg_integer()}.

-define(DEFAULT_READ_SINGLE_SHARD_STREAM_OPTS, #{
    limits => #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}},
        max_read_batches => 0
    }
}).

%%--------------------------------------------------------------------
%% Producer facade
%%--------------------------------------------------------------------

start_producer(Producer, ProducerOptions) ->
    case hstreamdb_producers_sup:start(Producer, ProducerOptions) of
        {ok, _Pid} ->
            ok;
        {error, _} = Error ->
            Error
    end.

stop_producer(Producer) ->
    hstreamdb_producers_sup:stop(Producer).

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

append(Producer, PartitioningKey, PayloadType, Payload) ->
    Record = to_record(PartitioningKey, PayloadType, Payload),
    append(Producer, Record).

append(Producer, Record) ->
    hstreamdb_producer:append(Producer, Record).

flush(Producer) ->
    hstreamdb_producer:flush(Producer).

append_flush(Producer, Record) ->
    hstreamdb_producer:append_flush(Producer, Record).

append_sync(Producer, Record) ->
    hstreamdb_producer:append_sync(Producer, Record).

%%--------------------------------------------------------------------
%% Client Manager facade
%%--------------------------------------------------------------------

start_client_manager(Client) ->
    start_client_manager(Client, #{}).

start_client_manager(Client, Options) ->
    hstreamdb_shard_client_mgr:start(Client, Options).

stop_client_manager(ClientManager) ->
    hstreamdb_shard_client_mgr:stop(ClientManager).

%%--------------------------------------------------------------------
%% Key Manager facade
%%--------------------------------------------------------------------

start_key_manager(Client, StreamName) ->
    start_key_manager(Client, StreamName, #{}).

start_key_manager(Client, StreamName, Options) ->
    hstreamdb_key_mgr:start(Client, StreamName, Options).

stop_key_manager(KeyManager) ->
    hstreamdb_key_mgr:stop(KeyManager).

%%--------------------------------------------------------------------
%% Reader single shard stream facade (simple, no pool)
%%--------------------------------------------------------------------

read_single_shard_stream(ClientManager, StreamName) ->
    read_single_shard_stream(ClientManager, StreamName, ?DEFAULT_READ_SINGLE_SHARD_STREAM_OPTS).

read_single_shard_stream(ClientManager, StreamName, Limits) ->
    Client = hstreamdb_shard_client_mgr:client(ClientManager),
    case hstreamdb_client:list_shards(Client, StreamName) of
        {ok, [#{shardId := ShardId}]} ->
            case hstreamdb_shard_client_mgr:lookup_client(ClientManager, ShardId) of
                {ok, ShardClient, NewClientManager} ->
                    case
                        hstreamdb_client:read_single_shard_stream(ShardClient, StreamName, Limits)
                    of
                        {ok, Result} ->
                            {ok, Result, NewClientManager};
                        {error, Reason} ->
                            {error, Reason, NewClientManager}
                    end;
                {error, _} = Error ->
                    Error
            end;
        {ok, L} when is_list(L) andalso length(L) > 1 ->
            {error, {multiple_shards, L}};
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Read multiple shard stream key facade
%%--------------------------------------------------------------------

-spec start_reader(ecpool:pool_name(), hstreamdb_reader:options()) -> ok | {error, term()}.
start_reader(Name, ReaderOptions) ->
    case hstreamdb_readers_sup:start(Name, ReaderOptions) of
        {ok, _Pid} ->
            ok;
        {error, _} = Error ->
            Error
    end.

-spec stop_reader(ecpool:pool_name()) -> ok.
stop_reader(Name) ->
    hstreamdb_readers_sup:stop(Name).

-spec read_stream_key(ecpool:pool_name(), partitioning_key(), limits()) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_stream_key(Name, Key, Limits) ->
    hstreamdb_reader:read(Name, Key, Limits).
