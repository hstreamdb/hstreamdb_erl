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
    start_client/1,
    start_client/2,
    stop_client/1
]).

-export([
    start_producer/2,
    stop_producer/1,
    to_record/3,
    append/2,
    append/4,
    append_async/2,
    append_async/4,
    flush/1,
    append_flush/2,
    append_flush/3,
    append_flush/4,
    append_sync/2,
    append_sync/3,
    append_sync/4
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
    start_reader/2,
    stop_reader/1,
    wait_for_reader/1,
    read_stream_key/3,
    read_stream_key/4
]).

-export_type([
    stream/0,
    partitioning_key/0,
    client/0,
    shard_id/0,
    hrecord/0
]).

-type stream() :: hstreamdb_client:stream().
-type partitioning_key() :: hstreamdb_client:partitioning_key().
-type client() :: hstreamdb_client:t().
-type shard_id() :: hstreamdb_client:shard_id().
-type hrecord() :: hstreamdb_client:hrecord().

-type limits_key() :: hstreamdb_client:limits_key().

-type reader_fold_acc() :: hstreamdb_client:reader_fold_acc().
-type reader_fold_fun() :: hstreamdb_client:reader_fold_fun().

%%--------------------------------------------------------------------
%% Client facade
%%--------------------------------------------------------------------

-spec start_client(hstreamdb_client:name(), hstreamdb_client:options() | proplists:proplist()) ->
    {ok, client()} | {error, term()}.
start_client(Name, Options) ->
    hstreamdb_client:start(Name, to_map(Options)).

-spec start_client(hstreamdb_client:options() | proplists:proplist()) ->
    {ok, client()} | {error, term()}.
start_client(Options) ->
    hstreamdb_client:start(to_map(Options)).

-spec stop_client(client() | hstreamdb_client:name()) -> ok.
stop_client(ClientOrName) ->
    hstreamdb_client:stop(ClientOrName).

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
    hstreamdb_batch_aggregator:append(Producer, Record).

append_async(Producer, PartitioningKey, PayloadType, Payload) ->
    Record = to_record(PartitioningKey, PayloadType, Payload),
    append_async(Producer, Record).

append_async(Producer, Record) ->
    hstreamdb_batch_aggregator:append_async(Producer, Record).

flush(Producer) ->
    hstreamdb_batch_aggregator:flush(Producer).

append_flush(Producer, Record) ->
    hstreamdb_batch_aggregator:append_flush(Producer, Record).

append_flush(Producer, Record, Timeout) ->
    hstreamdb_batch_aggregator:append_flush(Producer, Record, Timeout).

append_flush(Producer, Record, RecordTimeout, WriteTimeout) ->
    hstreamdb_batch_aggregator:append_flush(Producer, Record, RecordTimeout, WriteTimeout).

append_sync(Producer, Record) ->
    hstreamdb_batch_aggregator:append_sync(Producer, Record).

append_sync(Producer, Record, Timeout) ->
    hstreamdb_batch_aggregator:append_sync(Producer, Record, Timeout).

append_sync(Producer, Record, RecordTimeout, WriteTimeout) ->
    hstreamdb_batch_aggregator:append_sync(Producer, Record, RecordTimeout, WriteTimeout).

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
%% Auto Key Manager facade
%%--------------------------------------------------------------------

start_key_manager(Client, StreamName) ->
    start_key_manager(Client, StreamName, #{}).

start_key_manager(Client, StreamName, Options) ->
    hstreamdb_auto_key_mgr:start(Client, StreamName, Options).

stop_key_manager(KeyManager) ->
    hstreamdb_auto_key_mgr:stop(KeyManager).

%%--------------------------------------------------------------------
%% Start/stop key readers
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

-spec wait_for_reader(ecpool:pool_name()) -> ok | no_such_reader.
wait_for_reader(Name) ->
    hstreamdb_reader:wait_for_start(Name).

%% @doc fetch only records that have the same key as the one provided, using
%% server-side filtering.

-spec read_stream_key(ecpool:pool_name(), partitioning_key(), limits_key()) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_stream_key(Name, Key, Limits) ->
    hstreamdb_reader:read_key(Name, Key, Limits).

-spec read_stream_key(ecpool:pool_name(), partitioning_key(), limits_key(), {
    reader_fold_fun(), reader_fold_acc()
}) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_stream_key(Name, Key, Limits, Fold) ->
    hstreamdb_reader:read_key(Name, Key, Limits, Fold).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

to_map(Options) when is_map(Options) ->
    Options;
to_map(Options) when is_list(Options) ->
    maps:from_list(Options).
