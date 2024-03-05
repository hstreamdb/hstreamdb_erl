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

-module(hstreamdb_grpc_chan_sup).

-include("hstreamdb.hrl").

-behaviour(supervisor).

-export([start_link/4, spec/4, child_id/1]).

-export([init/1]).

start_link(ChanName, URL, Opts, Pid) ->
    supervisor:start_link(?MODULE, [ChanName, URL, Opts, Pid]).

init([ChanName, URL, Opts, Pid]) ->
    case grpc_client_sup:spec(ChanName, URL, Opts) of
        {ok, GrpcSupSpec} ->
            Specs = [GrpcSupSpec | monitor_spec(Pid, #{chan_name => ChanName, url => URL})],
            {ok, {
                #{
                    strategy => one_for_one,
                    intensity => 5,
                    period => 30,
                    auto_shutdown => any_significant
                },
                Specs
            }};
        {error, Reason} ->
            {error, Reason}
    end.

spec(ChanName, URL, Opts, Pid) ->
    #{
        id => child_id(ChanName),
        start => {?MODULE, start_link, [ChanName, URL, Opts, Pid]},
        restart => temporary,
        shutdown => infinity,
        type => supervisor
    }.

child_id(ChanName) -> {?MODULE, ChanName}.

monitor_spec(undefined, _Info) -> [];
monitor_spec(Pid, Info) -> [hstreamdb_grpc_monitor:spec(Pid, Info)].
