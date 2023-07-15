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

-module(hstreamdb_channel_reaper).

-behaviour(gen_server).

-export([start_link/0, register_channel/1, register_channel/2, unregister_channel/1, spec/0]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(REAP_TIMEOUT, 5000).

%%------------------------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------------------------

spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker
    }.

register_channel(ChannelName) ->
    register_channel(ChannelName, self()).

register_channel(ChannelName, Pid) ->
    gen_server:cast(?MODULE, {register, ChannelName, Pid}).

unregister_channel(ChannelName) ->
    gen_server:cast(?MODULE, {unregister, ChannelName}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%-------------------------------------------------------------------------------------------------
%% gen_server callbacks
%%-------------------------------------------------------------------------------------------------

init([]) ->
    _ = process_flag(trap_exit, true),
    {ok, #{
        pids_by_channel => #{}, channels_by_pid => #{}, monitors => #{}, reap_timers_by_pid => #{}
    }}.

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_call, Request}}, State}.

handle_cast({register, ChannelName, Pid}, #{pids_by_channel := PidsByChannel} = State) ->
    case maps:is_key(ChannelName, PidsByChannel) of
        true ->
            {noreply, State};
        false ->
            {noreply, do_register(ChannelName, Pid, State)}
    end;
handle_cast({unregister, ChannelName}, #{pids_by_channel := PidsByChannel} = State) ->
    case maps:is_key(ChannelName, PidsByChannel) of
        true ->
            {noreply, do_unregister(ChannelName, State)};
        false ->
            {noreply, State}
    end;
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(
    {'DOWN', _Ref, process, Pid, _Reason}, #{reap_timers_by_pid := ReapTimers0} = State0
) when
    is_map_key(Pid, ReapTimers0)
->
    ReapTimer = maps:get(Pid, ReapTimers0),
    _ = erlang:cancel_timer(ReapTimer),
    State1 = State0#{reap_timers_by_pid => maps:remove(Pid, ReapTimers0)},
    {noreply, State1};
handle_info({'DOWN', _Ref, process, Pid, _Reason}, #{channels_by_pid := ChannelsByPid} = State0) ->
    case maps:is_key(Pid, ChannelsByPid) of
        true ->
            ChannelName = maps:get(Pid, ChannelsByPid),
            State1 = do_unregister(ChannelName, State0),
            {noreply, do_reap_channel(ChannelName, State1)};
        false ->
            {noreply, State0}
    end;
handle_info({reap_timeout, Pid, MRef}, #{reap_timers_by_pid := ReapTimers0} = State0) ->
    _ = erlang:demonitor(MRef, [flush]),
    _ = exit(Pid, kill),
    State1 = State0#{reap_timers_by_pid => maps:remove(Pid, ReapTimers0)},
    {noreply, State1}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-------------------------------------------------------------------------------------------------
%% Internal functions
%%-------------------------------------------------------------------------------------------------

do_register(
    ChannelName,
    Pid,
    #{pids_by_channel := PidsByChannel0, channels_by_pid := ChannelsByPid0, monitors := Monitors0} =
        State
) ->
    PidsByChannel1 = maps:put(ChannelName, Pid, PidsByChannel0),
    ChannelsByPid1 = maps:put(Pid, ChannelName, ChannelsByPid0),
    Monitors1 = do_monitor(ChannelName, Pid, Monitors0),
    State#{
        pids_by_channel => PidsByChannel1, channels_by_pid => ChannelsByPid1, monitors => Monitors1
    }.

do_monitor(ChannelName, Pid, Monitors) ->
    Ref = erlang:monitor(process, Pid),
    maps:put(ChannelName, Ref, Monitors).

do_unregister(
    ChannelName,
    #{pids_by_channel := PidsByChannel0, channels_by_pid := ChannelsByPid0, monitors := Monitors0} =
        State
) ->
    Pid = maps:get(ChannelName, PidsByChannel0),
    PidsByChannel1 = maps:remove(ChannelName, PidsByChannel0),
    ChannelsByPid1 = maps:remove(Pid, ChannelsByPid0),
    Monitors1 = do_demonitor(ChannelName, Monitors0),
    State#{
        pids_by_channel => PidsByChannel1, channels_by_pid => ChannelsByPid1, monitors => Monitors1
    }.

do_demonitor(ChannelName, Monitors) ->
    Ref = maps:get(ChannelName, Monitors),
    erlang:demonitor(Ref, [flush]),
    maps:remove(ChannelName, Monitors).

do_reap_channel(ChannelName, #{reap_timers_by_pid := ReapTimers0} = State0) ->
    {Pid, MRef} = spawn_monitor(
        fun() ->
            case grpc_client_sup:stop_channel_pool(ChannelName) of
                ok ->
                    ok;
                Error ->
                    logger:error("[hstreamdb] error reaping GRPC channel ~p: ~p", [
                        ChannelName, Error
                    ])
            end
        end
    ),
    ReapTimerRef = erlang:send_after(?REAP_TIMEOUT, self(), {reap_timeout, Pid, MRef}),
    ReapTimers1 = maps:put(Pid, ReapTimerRef, ReapTimers0),
    State0#{reap_timers_by_pid => ReapTimers1}.
