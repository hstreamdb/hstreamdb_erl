%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% This supervisor is used to host all GRPC pools of a single producer/reader.
%%
%% The layout of the supervision tree is as follows:
%%
%% hstreamdb_producer_sup/hstreamdb_reader_sup
%% |
%% + ...other children...
%% |
%% + hstreamdb_grpc_sup(1): permanent
%%   |
%%   + hstreamdb_grpc_chan_sup(n): temporary, auto_shutdown=any_significant
%%     |                           One for each channel created by the producer/reader
%%     |
%%     + grpc_client_sup(1): transient
%%     |
%%     + hstreamdb_grpc_monitor(1): temporary, significant child
%%
%% hstreamdb_grpc_monitor monitors the pid of the procsses that created the pool.
%% If the process exits, the monitor exits too, and the whole
%% `hstreamdb_grpc_chan_sup` supervision subtree is shut down.

-module(hstreamdb_grpc_sup).

-include("hstreamdb.hrl").

-behaviour(supervisor).

-export([
    start_link/1,
    spec/1,
    sup_ref/1,
    start_supervised/5,
    stop_supervised/2
]).

-export([init/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Name) ->
    supervisor:start_link(sup_ref(Name), ?MODULE, []).

spec(Name) ->
    #{
        id => child_id(Name),
        start => {?MODULE, start_link, [Name]},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    }.

sup_ref(Name) -> ?VIA_GPROC(id(Name)).

start_supervised(SupRef, ChanName, URL, Opts, Pid) ->
    supervisor:start_child(
        SupRef,
        hstreamdb_grpc_chan_sup:spec(ChanName, URL, Opts, Pid)
    ).

stop_supervised(SupRef, ChanName) ->
    ChildId = hstreamdb_grpc_chan_sup:child_id(ChanName),
    %% The child is temporary, so no need to delete it from the supervisor
    %% after termination.
    supervisor:terminate_child(SupRef, ChildId).


%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {#{strategy => one_for_one, intensity => 5, period => 30}, []}}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

child_id(Name) -> id(Name).

id(Name) -> {?MODULE, Name}.
