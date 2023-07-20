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
    start_producer/3,
    stop_producer/1,
    to_record/3,
    append/2,
    append/4,
    flush/1,
    append_flush/2
]).

-export([
    start_client_manager/1,
    start_client_manager/2,
    stop_client_manager/1,

    start_key_manager/2,
    start_key_manager/3,
    stop_key_manager/1,

    read_single_shard_stream/2,
    read_single_shard_stream/3,

    read_stream_key/3,
    read_stream_key/4
]).

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

start_producer(Client, Producer, ProducerOptions) ->
    case hstreamdb_producers_sup:start(Producer, [{client, Client} | ProducerOptions]) of
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

append_flush(Producer, Data) ->
    hstreamdb_producer:append_flush(Producer, Data).

%%--------------------------------------------------------------------
%% Stream reader facade
%%--------------------------------------------------------------------

start_client_manager(Client) ->
    start_client_manager(Client, #{}).

start_client_manager(Client, Options) ->
    hstreamdb_shard_client_mgr:start(Client, Options).

stop_client_manager(ClientManager) ->
    hstreamdb_shard_client_mgr:stop(ClientManager).

start_key_manager(Client, StreamName) ->
    start_key_manager(Client, StreamName, []).

start_key_manager(Client, StreamName, Options) ->
    hstreamdb_key_mgr:start(Client, StreamName, Options).

stop_key_manager(KeyManager) ->
    hstreamdb_key_mgr:stop(KeyManager).

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

read_stream_key(ClientManager, KeyManager, Key) ->
    DefaultLimits = maps:get(limits, ?DEFAULT_READ_SINGLE_SHARD_STREAM_OPTS),
    read_stream_key(ClientManager, KeyManager, Key, DefaultLimits).

read_stream_key(ClientManager, KeyManager, Key, Limits) ->
    {ShardId, NewKeyManager} = hstreamdb_key_mgr:choose_shard(KeyManager, Key),
    case hstreamdb_shard_client_mgr:lookup_client(ClientManager, ShardId) of
        {ok, ShardClient, NewClientManager} ->
            Opts = #{
                fold => {fold_stream_key(Key), []},
                limits => Limits
            },
            case hstreamdb_client:read_shard_stream(ShardClient, ShardId, Opts) of
                {ok, Result} ->
                    {ok, Result, NewClientManager, NewKeyManager};
                {error, Reason} ->
                    {error, Reason, NewClientManager, NewKeyManager}
            end;
        {error, _} = Error ->
            {error, Error, ClientManager, NewKeyManager}
    end.

fold_stream_key(Key) ->
    BinKey = iolist_to_binary(Key),
    fun
        (#{header := #{key := PK}} = Record, Acc) when BinKey =:= PK -> [Record | Acc];
        (_, Acc) -> Acc
    end.
