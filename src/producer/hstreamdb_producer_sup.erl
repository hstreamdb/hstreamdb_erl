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

-type options() :: #{
    stream := hstreamdb:stream(),
    mgr_client_options := hstreamdb_client:options(),
    buffer_pool_size => non_neg_integer(),
    buffer_options => hstreamdb_producer:options(),
    writer_options => hstreamdb_batch_writer:options(),
    writer_pool_size => non_neg_integer(),
    stop_timeout => non_neg_integer()
}.

-spec start_link(ecpool:pool_name(), options()) -> {ok, pid()}.
start_link(Producer, Opts) ->
    supervisor:start_link(?MODULE, [Producer, Opts]).

init([
    Producer,
    #{
        stream := Stream,
        mgr_client_options := MgrClientOptions
    } = Opts
]) ->
    BufferPoolSize = maps:get(buffer_pool_size, Opts, ?DEFAULT_BUFFER_POOL_SIZE),
    BufferOptions0 = maps:get(buffer_options, Opts, #{}),
    BufferOptions = BufferOptions0#{
        stream => Stream,
        producer_name => Producer,
        mgr_client_options => MgrClientOptions
    },

    WriterPoolSize = maps:get(writer_pool_size, Opts, ?DEFAULT_WRITER_POOL_SIZE),
    WriterOptions0 = maps:get(writer_options, Opts, #{}),
    WriterOptions = WriterOptions0#{
        stream => Stream,
        mgr_client_options => MgrClientOptions
    },
    StopTimeout = maps:get(stop_timeout, Opts, ?DEFAULT_STOP_TIMEOUT),

    ChildSpecs = [
        buffer_pool_spec(Producer, BufferPoolSize, BufferOptions),
        writer_pool_spec(Producer, WriterPoolSize, WriterOptions),
        terminator_spec(Producer, StopTimeout)
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 30}, ChildSpecs}}.

buffer_pool_spec(Producer, PoolSize, Opts) ->
    PoolOpts = [{pool_size, PoolSize}, {opts, Opts}],
    ecpool_spec(Producer, hstreamdb_producer, PoolOpts).

writer_pool_spec(Producer, PoolSize, Opts) ->
    WriterOptions = [{pool_size, PoolSize}, {opts, Opts}],
    ecpool_spec(hstreamdb_producer:writer_name(Producer), hstreamdb_batch_writer, WriterOptions).

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
