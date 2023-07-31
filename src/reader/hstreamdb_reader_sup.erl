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
%% Supervisor for reader
%%--------------------------------------------------------------------

-module(hstreamdb_reader_sup).

-include("hstreamdb.hrl").

-behaviour(supervisor).

-export([start_link/2, spec/2, child_id/1]).
-export([init/1]).

start_link(Reader, Opts) ->
    supervisor:start_link(?MODULE, [Reader, Opts]).

init([Reader, Opts]) ->
    PoolSize = maps:get(pool_size, Opts, ?DEFAULT_READER_POOL_SIZE),
    EcpoolOpts = [{pool_size, PoolSize}, {reader_options, Opts}],
    ChildSpecs = [
        pool_spec(Reader, EcpoolOpts)
    ],
    {ok, {#{strategy => one_for_one, intensity => 5, period => 30}, ChildSpecs}}.

pool_spec(Reader, Opts) ->
    ecpool_spec(Reader, hstreamdb_reader, Opts).

spec(Reader, Opts) ->
    #{
        id => child_id(Reader),
        start => {?MODULE, start_link, [Reader, Opts]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.

child_id(Reader) ->
    {?MODULE, Reader}.

ecpool_spec(Pool, Mod, Opts) ->
    #{
        id => {pool_sup, Pool},
        start => {ecpool_pool_sup, start_link, [Pool, Mod, Opts]},
        restart => transient,
        shutdown => infinity,
        type => supervisor
    }.
