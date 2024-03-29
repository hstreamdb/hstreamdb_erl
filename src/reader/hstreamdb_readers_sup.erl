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
%% Supervisor for all readers
%%--------------------------------------------------------------------

-module(hstreamdb_readers_sup).

-behaviour(supervisor).

-export([start/2, stop/1]).

-export([start_link/0, spec/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 30}, []}}.

spec() ->
    #{
        id => ?SERVER,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.

start(Reader, Opts) ->
    supervisor:start_child(?SERVER, hstreamdb_reader_sup:spec(Reader, Opts)).

stop(Reader) ->
    ChildId = hstreamdb_reader_sup:child_id(Reader),
    case supervisor:terminate_child(?SERVER, ChildId) of
        ok ->
            supervisor:delete_child(?SERVER, ChildId);
        {error, Reason} ->
            {error, Reason}
    end.
