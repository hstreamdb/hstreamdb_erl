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
%% Supervisor for all producers
%%--------------------------------------------------------------------

-module(hstreamdb_producer_sup).

-include("hstreamdb.hrl").

-behaviour(supervisor).

-export([start_link/2, spec/2, child_id/1]).
-export([init/1]).

start_link(Producer, Opts) ->
    supervisor:start_link(?MODULE, [Producer, Opts]).

init([Producer, Opts]) ->
    ChildSpecs = [
        buffer_pool_spec(Producer, Opts),
        writer_pool_spec(Producer, Opts),
        terminator_spec(Producer, Opts)
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 30}, ChildSpecs}}.

buffer_pool_spec(Producer, Opts) ->
    BufferOpts = [{producer_name, Producer} | Opts],
    ecpool_spec(Producer, hstreamdb_producer, BufferOpts).

writer_pool_spec(Producer, Opts) ->
    WriterPoolSise = proplists:get_value(
        writer_pool_size, Opts, ?DEFAULT_WRITER_POOL_SIZE
    ),
    WriterOptions = [{pool_size, WriterPoolSise} | proplists:delete(pool_size, Opts)],
    ecpool_spec(hstreamdb_producer:writer_name(Producer), hstreamdb_batch_writer, WriterOptions).

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

terminator_spec(Producer, Opts) ->
    StopTimeout = proplists:get_value(stop_timeout, Opts, ?DEFAULT_STOP_TIMEOUT),
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
