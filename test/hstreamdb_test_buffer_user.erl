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

-module(hstreamdb_test_buffer_user).

-behaviour(gen_server).

-export([
    start_link/1,
    append/3,
    flush/1,
    stop/1,
    is_empty/1,
    wait_for_empty/2
]).

-export([
    init/1,
    handle_cast/2,
    handle_info/2,
    handle_call/3
]).

-record(st, {
    buffer :: hstreamdb_buffer:hstreamdb_buffer()
}).

-define(DEFAULT_FLUSH_INTERVAL, 10).
-define(DEFAULT_BATCH_TIMEOUT, 10).
-define(DEFAULT_BATCH_SIZE, 10).
-define(DEFAULT_BATCH_MAX_COUNT, 99999999).

-define(BATCH_LOSE_RATE, 0.1).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(Tab) ->
    gen_server:start_link(?MODULE, [Tab], []).

init([Tab]) ->
    {ok, #st{buffer = new_buffer(Tab)}}.

append(Pid, N, Rec) ->
    gen_server:cast(Pid, {append, {self(), N}, Rec}).

stop(Pid) ->
    gen_server:call(Pid, stop).

flush(Pid) ->
    gen_server:cast(Pid, flush).

is_empty(Pid) ->
    gen_server:call(Pid, is_empty).

wait_for_empty(_Pid, 0) -> false;
wait_for_empty(Pid, NSecs) when NSecs > 0 ->
    case is_empty(Pid) of
        true ->
            true;
        false ->
            timer:sleep(1000),
            wait_for_empty(Pid, NSecs - 1)
    end.

%%--------------------------------------------------------------------
%% Gen Server Callbacks
%%--------------------------------------------------------------------

handle_cast({append, From, Rec}, #st{buffer = Buffer} = St) ->
    {ok, NewBuffer} = hstreamdb_buffer:append(Buffer, From, [Rec], infinity),
    {noreply, St#st{buffer = NewBuffer}};

handle_cast(flush, #st{buffer = Buffer} = St) ->
    NewBuffer = hstreamdb_buffer:flush(Buffer),
    {noreply, St#st{buffer = NewBuffer}}.

handle_info({buffer_event, Event}, #st{buffer = Buffer} = St) ->
    NewBuffer = hstreamdb_buffer:handle_event(Buffer, Event),
    {noreply, St#st{buffer = NewBuffer}};
handle_info({send_batch, #{batch_ref := Ref}}, #st{buffer = Buffer} = St) ->
    case need_lose_batch() of
        true ->
            {noreply, St};
        false ->
            NewBuffer = hstreamdb_buffer:handle_event(Buffer, {batch_response, Ref, ok}),
            {noreply, St#st{buffer = NewBuffer}}
    end.

handle_call(is_empty, _From, #st{buffer = Buffer} = St) ->
    {reply, hstreamdb_buffer:is_empty(Buffer), St};
handle_call(stop, _From, St) ->
    {stop, normal, ok, St}.

need_lose_batch() ->
    rand:uniform() < ?BATCH_LOSE_RATE.

new_buffer(BatchTab) ->
    Opts = #{
        flush_interval => ?DEFAULT_FLUSH_INTERVAL,
        batch_timeout => ?DEFAULT_BATCH_TIMEOUT,
        batch_size => ?DEFAULT_BATCH_SIZE,
        batch_max_count => ?DEFAULT_BATCH_MAX_COUNT,

        batch_tab => BatchTab,

        send_batch => fun(Batch) ->
            self() ! {send_batch, Batch},
            ok
        end,
        send_after => fun(Timeout, Message) ->
            erlang:send_after(Timeout, self(), {buffer_event, Message})
        end,
        cancel_send => fun(Ref) ->
            erlang:cancel_timer(Ref)
        end,
        send_reply => fun(From, Response) ->
            {Pid, N} = From,
            Pid ! {N, Response}
        end
    },
    hstreamdb_buffer:new(Opts).

