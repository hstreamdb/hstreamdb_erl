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
%%
%% @doc
%% Supervisor for producer
%%--------------------------------------------------------------------

-module(hstreamdb_producer_sup).

-include("hstreamdb.hrl").

-behaviour(supervisor).

-export([start_link/2, spec/2, child_id/1]).
-export([init/1]).

-type batch_writer_options() :: #{
    grpc_timeout => non_neg_integer(),
    auto_reconnect => false | pos_integer()
}.

-type batch_aggregator_options() :: #{
    callback := hstreamdb_batch_aggregator:callback(),
    interval => pos_integer(),
    batch_reap_timeout => pos_integer(),
    max_records => pos_integer(),
    max_batches => pos_integer(),
    compression_type => none | gzip | zstd,
    discovery_backoff_opts => hstreamdb_backoff:options(),
    auto_reconnect => false | pos_integer()
}.

-type discovery_options() :: #{
    backoff_options := hstreamdb_backoff:options(),
    min_active_time => non_neg_integer()
}.

-type options() :: #{
    stream := hstreamdb:stream(),
    client_options := hstreamdb_client:options(),
    buffer_pool_size => non_neg_integer(),
    buffer_options => batch_aggregator_options(),
    writer_options => batch_writer_options(),
    writer_pool_size => non_neg_integer(),
    discovery_options => discovery_options(),
    stop_timeout => non_neg_integer()
}.

-spec start_link(ecpool:pool_name(), options()) -> {ok, pid()}.
start_link(Producer, Opts) ->
    supervisor:start_link(?MODULE, [Producer, Opts]).

init([
    Producer,
    #{
        stream := Stream,
        client_options := ClientOptions
    } = Opts
]) ->
    BufferPoolSize = maps:get(buffer_pool_size, Opts, ?DEFAULT_BUFFER_POOL_SIZE),
    BufferOptions0 = maps:get(buffer_options, Opts, #{}),
    BufferOptions = BufferOptions0#{
        stream => Stream,
        name => Producer
    },

    WriterPoolSize = maps:get(writer_pool_size, Opts, ?DEFAULT_WRITER_POOL_SIZE),
    WriterOptions0 = maps:get(writer_options, Opts, #{}),
    WriterOptions = WriterOptions0#{
        stream => Stream,
        name => Producer
    },

    DiscoveryOptions0 = maps:get(discovery_options, Opts, #{}),
    DiscoveryOptions = DiscoveryOptions0#{
        stream => Stream,
        name => Producer,
        client_options => ClientOptions,

        on_init => [
            fun hstreamdb_batch_writer:on_init/1,
            fun hstreamdb_batch_aggregator:on_init/1
        ],
        on_stream_updated => [fun hstreamdb_batch_aggregator:on_stream_updated/3],
        on_shards_updated => [fun hstreamdb_batch_writer:on_shards_updated/3],
        on_terminate => [
            fun hstreamdb_batch_writer:on_terminate/4,
            fun hstreamdb_batch_aggregator:on_terminate/4
        ]
    },
    StopTimeout = maps:get(stop_timeout, Opts, ?DEFAULT_STOP_TIMEOUT),

    ChildSpecs = [
        batch_aggregator_pool_spec(Producer, BufferPoolSize, BufferOptions),
        batch_writer_pool_spec(Producer, WriterPoolSize, WriterOptions),
        discovery_spec(DiscoveryOptions),
        terminator_spec(Producer, StopTimeout)
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 30}, ChildSpecs}}.

discovery_spec(Opts) ->
    #{
        id => discovery,
        start => {hstreamdb_discovery, start_link, [Opts]},
        restart => permanent,
        shutdown => infinity,
        type => worker
    }.

batch_aggregator_pool_spec(Producer, PoolSize, Opts) ->
    AutoReconnect = maps:get(auto_reconnect, Opts, ?DEAULT_AUTO_RECONNECT),
    PoolOpts = [{pool_size, PoolSize}, {pool_type, hash}, {auto_reconnect, AutoReconnect}, {opts, Opts}],
    ecpool_spec(Producer, hstreamdb_batch_aggregator, PoolOpts).

batch_writer_pool_spec(Producer, PoolSize, Opts) ->
    AutoReconnect = maps:get(auto_reconnect, Opts, ?DEAULT_AUTO_RECONNECT),
    WriterOptions = [
        {pool_size, PoolSize}, {opts, Opts}, {auto_reconnect, AutoReconnect}
    ],
    ecpool_spec(hstreamdb_batch_aggregator:writer_name(Producer), hstreamdb_batch_writer, WriterOptions).

terminator_spec(Producer, StopTimeout) ->
    #{
        id => terminator,
        start =>
            {hstreamdb_producer_terminator, start_link, [
                #{producer => Producer, timeout => StopTimeout}
            ]},
        restart => permanent,
        shutdown => StopTimeout + 1000,
        type => worker
    }.

spec(Producer, Opts) ->
    #{
        id => child_id(Producer),
        start => {?MODULE, start_link, [Producer, Opts]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.

child_id(Producer) ->
    {?MODULE, Producer}.

ecpool_spec(Pool, Mod, Opts) ->
    #{
        id => {pool_sup, Pool},
        start => {ecpool_pool_sup, start_link, [Pool, Mod, Opts]},
        restart => transient,
        shutdown => infinity,
        type => supervisor
    }.
