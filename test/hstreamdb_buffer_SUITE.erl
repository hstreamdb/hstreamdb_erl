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

-module(hstreamdb_buffer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include("assert.hrl").

-define(DEFAULT_FLUSH_INTERVAL, 100).
-define(DEFAULT_BATCH_TIMEOUT, 100).
-define(DEFAULT_BATCH_SIZE, 10).
-define(DEFAULT_BATCH_MAX_COUNT, 5).

all() ->
    hstreamdb_test_helpers:test_cases(?MODULE).

init_per_suite(Config) ->
    Config.
end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    BatchTab = ets:new(?MODULE, [public]),
    [{batch_tab, BatchTab} | Config].

end_per_testcase(_TestCase, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_simple_append(Config) ->
    Buffer = new_buffer(Config),

    {ok, _NewBuffer} = hstreamdb_buffer:append(Buffer, from, [<<"rec">>], infinity),

    ?assertReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}).

t_batch_too_large(Config) ->
    Buffer = new_buffer(Config),

    Recs = [<<"rec">> || _ <- lists:seq(1, ?DEFAULT_BATCH_SIZE * ?DEFAULT_BATCH_MAX_COUNT)],

    ?assertMatch(
        {error, _},
        hstreamdb_buffer:append(Buffer, from, [<<"rec">> | Recs], infinity)
    ),

    ?assertMatch(
        {ok, _},
        hstreamdb_buffer:append(Buffer, from, Recs, infinity)
    ).

t_is_empty(Config) ->
    Buffer0 = new_buffer(Config),
    ?assert(hstreamdb_buffer:is_empty(Buffer0)),

    Buffer1 = hstreamdb_buffer:append(Buffer0, from, [<<"rec">>], infinity),
    ?assertNot(hstreamdb_buffer:is_empty(Buffer1)).

t_append_batch(Config) ->
    Buffer = new_buffer(Config),

    Recs = [<<"rec">> || _ <- lists:seq(1, ?DEFAULT_BATCH_SIZE)],

    {ok, _NewBuffer} = hstreamdb_buffer:append(Buffer, from, Recs, infinity),

    ?refuteReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}),

    ?assertReceive({send_after, ?DEFAULT_BATCH_TIMEOUT, {batch_timeout, _}}),
    ?assertReceive({send_batch, #{batch_ref := _, tab := _}}).

t_flush_by_timer(Config) ->
    Buffer0 = new_buffer(Config),

    Recs = [<<"rec">>],

    {ok, Buffer1} = hstreamdb_buffer:append(Buffer0, from, Recs, infinity),

    ?assertReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}),

    Buffer2 = hstreamdb_buffer:handle_event(Buffer1, flush),
    ?assertReceive({send_after, ?DEFAULT_BATCH_TIMEOUT, {batch_timeout, _}}),
    ?assertReceive({send_batch, #{batch_ref := _, tab := _}}),
    ?refuteReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}),

    {ok, _Buffer3} = hstreamdb_buffer:append(Buffer2, from, Recs, infinity),
    ?assertReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}).

t_explicit_flush(Config) ->
    Buffer0 = new_buffer(Config),
    Recs = [<<"rec">>],

    {ok, Buffer1} = hstreamdb_buffer:append(Buffer0, from, Recs, infinity),
    ?assertReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}),

    Buffer2 = hstreamdb_buffer:flush(Buffer1),

    ?refuteReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}),
    ?assertReceive({send_after, ?DEFAULT_BATCH_TIMEOUT, {batch_timeout, _}}),
    ?assertReceive({send_batch, #{batch_ref := _, tab := _}}),

    Buffer3 = hstreamdb_buffer:flush(Buffer2),

    ?refuteReceive({send_after, ?DEFAULT_BATCH_TIMEOUT, {batch_timeout, _}}),
    ?refuteReceive({send_batch, #{batch_ref := _, tab := _}}),

    {ok, Buffer4} = hstreamdb_buffer:append(Buffer3, from, Recs, infinity),

    ?assertReceive({send_after, ?DEFAULT_FLUSH_INTERVAL, flush}),

    _Buffer5 = hstreamdb_buffer:flush(Buffer4),

    %% There is an inflight batch, so new batch is not sent and
    %% we should not receive batch_timeout message

    ?refuteReceive({send_after, ?DEFAULT_BATCH_TIMEOUT, {batch_timeout, _}}),
    ?refuteReceive({send_batch, _}).

t_batches_with_responses(Config) ->
    Buffer0 = new_buffer(Config),

    Recs = [<<"rec">> || _ <- lists:seq(1, ?DEFAULT_BATCH_SIZE * 3)],

    {ok, Buffer1} = hstreamdb_buffer:append(Buffer0, from, Recs, infinity),

    lists:foldl(
        fun(_, Buffer) ->
            receive
                {send_batch, #{batch_ref := Ref, tab := _Tab}} ->
                    NewBuffer = hstreamdb_buffer:handle_event(Buffer, {batch_response, Ref, ok}),
                    lists:foreach(
                        fun(_) ->
                            ?assertReceive({send_reply, from, ok})
                        end,
                        lists:seq(1, ?DEFAULT_BATCH_SIZE)
                    ),
                    NewBuffer
            after 100 ->
                ct:fail("Did not receive send_batch message")
            end
        end,
        Buffer1,
        lists:seq(1, 3)
    ).

t_batches_with_timeouts(Config) ->
    Buffer0 = new_buffer(Config),

    Recs = [<<"rec">> || _ <- lists:seq(1, ?DEFAULT_BATCH_SIZE * 3)],

    {ok, Buffer1} = hstreamdb_buffer:append(Buffer0, from, Recs, infinity),

    lists:foldl(
        fun(_, Buffer) ->
            receive
                {send_batch, #{batch_ref := Ref, tab := _Tab}} ->
                    NewBuffer = hstreamdb_buffer:handle_event(Buffer, {batch_timeout, Ref}),
                    lists:foreach(
                        fun(_) ->
                            ?assertReceive({send_reply, from, {error, timeout}})
                        end,
                        lists:seq(1, ?DEFAULT_BATCH_SIZE)
                    ),
                    NewBuffer
            after 100 ->
                ct:fail("Did not receive send_batch message")
            end
        end,
        Buffer1,
        lists:seq(1, 3)
    ).

t_reply_callback_exception(Config) ->
    Buffer0 = new_buffer(Config),

    {ok, Buffer1} = hstreamdb_buffer:append(Buffer0, from0, [<<"rec">>], infinity),
    {ok, Buffer2} = hstreamdb_buffer:append(Buffer1, fun(_) -> error(oops) end, [<<"rec">>], infinity),
    {ok, Buffer3} = hstreamdb_buffer:append(Buffer2, from1, [<<"rec">>], infinity),

    Buffer4 = hstreamdb_buffer:flush(Buffer3),

    receive
        {send_batch, #{batch_ref := Ref, tab := _Tab}} ->
            _ = hstreamdb_buffer:handle_event(Buffer4, {batch_response, Ref, ok}),
            ?assertReceive({send_reply, from0, ok}),
            ?assertReceive({send_reply, from1, ok})
    after 100 ->
        ct:fail("Did not receive send_batch message")
    end.

t_response_after_deadline(Config) ->
    Buffer0 = new_buffer(Config),

    {ok, Buffer1} = hstreamdb_buffer:append(Buffer0, from1, [<<"rec">>], 1),
    {ok, Buffer2} = hstreamdb_buffer:append(Buffer1, from2, [<<"rec">>], 1000),
    Buffer3 = hstreamdb_buffer:flush(Buffer2),
    ok = timer:sleep(2),
    receive
        {send_batch, #{batch_ref := Ref, tab := _Tab}} ->
            _ = hstreamdb_buffer:handle_event(Buffer3, {batch_response, Ref, ok}),
            ?refuteReceive({send_reply, from1, _}),
            ?assertReceive({send_reply, from2, ok})
    after 100 ->
        ct:fail("Did not receive send_batch message")
    end.


%% We send 10000 records, send occasional flushes.
%% Client timeouts some batches.
%% We should receive all responses (ok or {error, timeout})
%% We also check that tab is empty at the end.

%% PropEr?
t_real_client(Config) ->
    {ok, Pid} = hstreamdb_test_buffer_user:start_link(?config(batch_tab, Config)),

    NRecs = 10000,
    FlushRate = 0.03,

    ok = lists:foreach(
        fun(N) ->
            ok = hstreamdb_test_buffer_user:append(Pid, N, <<"rec">>),
            case need_flush(FlushRate) of
                true ->
                    ok = hstreamdb_test_buffer_user:flush(Pid);
                false ->
                    ok
                end
        end,
    lists:seq(1, NRecs)),
    ?assert(hstreamdb_test_buffer_user:wait_for_empty(Pid, 5)),

    ok = assert_receive_all_responses(NRecs),

    ?assertEqual(
        0,
        ets:info(?config(batch_tab, Config), size)
    ),

    ok = hstreamdb_test_buffer_user:stop(Pid).


%%--------------------------------------------------------------------
%% Helper Functions
%%--------------------------------------------------------------------

new_buffer(Config) ->
    new_buffer(Config, #{}).

new_buffer(Config, Opts) ->
    BatchTab = ?config(batch_tab, Config),
    DefaultOpts = #{
        flush_interval => ?DEFAULT_FLUSH_INTERVAL,
        batch_timeout => ?DEFAULT_BATCH_TIMEOUT,
        batch_size => ?DEFAULT_BATCH_SIZE,
        batch_max_count => ?DEFAULT_BATCH_MAX_COUNT,

        batch_tab => BatchTab,

        send_batch => fun(Message) ->
            self() ! {send_batch, Message},
            ok
        end,
        send_after => fun(Timeout, Message) ->
            self() ! {send_after, Timeout, Message},
            make_ref()
        end,
        cancel_send => fun(Ref) ->
            self() ! {cancel_send, Ref}
        end,
        send_reply => fun(From, Response) ->
            self() ! {send_reply, From, Response}
        end
    },
    hstreamdb_buffer:new(
        maps:merge(DefaultOpts, Opts)
    ).

need_flush(FlushRate) ->
    rand:uniform() < FlushRate.

assert_receive_all_responses(0) ->
    ok;
assert_receive_all_responses(N) when N > 0 ->
    ?assertReceived({N, _}),
    assert_receive_all_responses(N - 1).

