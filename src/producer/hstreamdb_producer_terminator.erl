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

-module(hstreamdb_producer_terminator).

-behaviour(gen_server).

-export([start_link/1]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(POOL_TIMEOUT, 5000).

%%------------------------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

%%-------------------------------------------------------------------------------------------------
%% gen_server callbacks
%%-------------------------------------------------------------------------------------------------

init([#{producer := _, timeout := _} = Opts]) ->
    _ = process_flag(trap_exit, true),
    {ok, Opts}.

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_call, Request}}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    do_terminate(State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-------------------------------------------------------------------------------------------------
%% Internal functions
%%-------------------------------------------------------------------------------------------------

do_terminate(#{producer := Producer, timeout := Timeout}) ->
    Self = alias(),
    Workers = ecpool:workers(Producer),
    ok = notify_stop(Self, Workers),
    NWorkers = length(Workers),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_for_terminate(Self, NWorkers, Deadline).

notify_stop(Self, Workers) ->
    lists:foreach(
        fun({_Name, WPid}) ->
            ecpool_worker:exec(
                WPid,
                fun(Pid) ->
                    gen_server:cast(Pid, {stop, Self})
                end,
                ?POOL_TIMEOUT
            )
        end,
        Workers
    ).

%% This process is about to terminate
%% we don't care about unalias/cleaning the process queue in this function
wait_for_terminate(_Self, 0, _Deadline) ->
    ok;
wait_for_terminate(Self, NWorkers, Deadline) when NWorkers > 0 ->
    TimeLeft = to_nonegative(Deadline - erlang:monotonic_time(millisecond) + 1),
    receive
        {empty, Self} ->
            wait_for_terminate(Self, NWorkers - 1, Deadline)
    after TimeLeft ->
        logger:error("[hstreamdb] producer terminator timeout, still have ~p workers", [NWorkers]),
        ok
    end.

to_nonegative(Value) when Value < 0 -> 0;
to_nonegative(Value) -> Value.
