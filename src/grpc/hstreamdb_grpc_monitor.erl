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

-module(hstreamdb_grpc_monitor).

-behaviour(gen_server).

-export([start_link/2, spec/2]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(pid(), term()) -> gen_server:start_ret().
start_link(Pid, Info) ->
    gen_server:start_link(?MODULE, [Pid, Info], []).

-spec spec(pid(), term()) -> supervisor:child_spec().
spec(Pid, Info) ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, [Pid, Info]},
        restart => temporary,
        shutdown => 5000,
        significant => true,
        type => worker
    }.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pid, Info]) ->
    MRef = monitor(process, Pid),
    {ok, #{mref => MRef, pid => Pid, info => Info}}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'DOWN', MRef, process, Pid, _Reason}, #{mref := MRef, pid := Pid, info := Info} = State) ->
    {stop, {error, {grpc_pool_owner_died, Info#{pid => Pid}}}, State};

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
