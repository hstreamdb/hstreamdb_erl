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

-module(hstreamdb_producer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("assert.hrl").
-include("errors.hrl").

-define(DAY, (24 * 60 * 60)).

-define(STREAM, "stream2").

-define(assertResult(Result), begin
    receive
        {producer_result, {{flush, ?STREAM, 1}, Result}} ->
            ok
    after 2000 ->
        ct:fail("producer result not received, inbox: ~p", [hstreamdb_test_helpers:drain_inbox()])
    end
end).

-define(assertOkResult(), ?assertResult({ok, #{}})).

all() ->
    hstreamdb_test_helpers:test_cases(?MODULE).

init_per_suite(Config) ->
    _ = application:ensure_all_started(hstreamdb_erl),
    Config.
end_per_suite(_Config) ->
    _ = application:stop(hstreamdb_erl),
    ok.

init_per_testcase(_TestCase, Config) ->
    Client = hstreamdb_test_helpers:client(test_c),
    _ = hstreamdb_client:delete_stream(Client, ?STREAM),
    ok = hstreamdb_client:create_stream(Client, ?STREAM, 2, ?DAY, 5),
    _ = hstreamdb_test_helpers:reset_proxy(),
    _ = snabbkaffe:start_trace(),
    [{producer_name, test_producer}, {client, Client} | Config].
end_per_testcase(_TestCase, Config) ->
    _ = snabbkaffe:stop(),
    try
        hstreamdb:stop_producer(?config(producer_name, Config))
    catch
        Error:Reason ->
            ct:print("stop producer error: ~p:~p~n", [Error, Reason])
    end,
    Client = ?config(client, Config),
    _ = hstreamdb_client:delete_stream(Client, ?STREAM),
    _ = hstreamdb_client:stop(Client),
    ok.

t_start_stop(Config) ->
    ProducerOptions = #{},

    ?assertEqual(
        ok,
        start_producer(Config, ProducerOptions)
    ),

    ?assertMatch(
        {error, _},
        start_producer(Config, ProducerOptions)
    ),

    ?assertEqual(
        ok,
        hstreamdb:stop_producer(?config(producer_name, Config))
    ).

t_flush_by_timeout(Config) ->
    ProducerOptions = #{
        buffer_options => #{
            max_records => 10000,
            interval => 100
        }
    },

    ok = start_producer(Config, ProducerOptions),

    ?assertEqual(
        ok,
        hstreamdb:append(producer(Config), sample_record())
    ),

    ?assertOkResult().

t_flush_explicit(Config) ->
    ProducerOptions = #{
        buffer_options => #{
            max_records => 10000,
            interval => 10000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    ?assertEqual(
        ok,
        hstreamdb:append(producer(Config), sample_record())
    ),

    ok = hstreamdb:flush(producer(Config)),

    ?assertOkResult().

t_append_flush_no_callback(Config) ->
    ProducerOptions = #{
        buffer_options => #{
            max_records => 10000,
            interval => 10000,
            callback => fun(_Result) ->
                ct:fail("callback should not be called for append_flush")
            end
        }
    },

    ok = start_producer(Config, ProducerOptions),

    ?assertMatch(
        {ok, #{}},
        hstreamdb:append_flush(producer(Config), sample_record())
    ).

t_flush_by_limit(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 20,
            interval => 10000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    lists:foreach(
        fun(_) ->
            ok = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, 30)
    ),

    ok = hstreamdb:flush(producer(Config)),

    ok = assert_ok_flush_result(20).

t_append(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10000,
            interval => 10000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    lists:foreach(
        fun(_) ->
            ok = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, 100)
    ),

    ok = hstreamdb:flush(producer(Config)),

    ok = assert_ok_flush_result(100).

t_append_many(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10000,
            interval => 10000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    Records = [sample_record() || _ <- lists:seq(1, 100)],
    ok = hstreamdb:append(producer(Config), Records),

    ok = hstreamdb:flush(producer(Config)),
    ok = assert_ok_flush_result(100).

t_overflooded_queue(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 1,
            max_batches => 1,
            interval => 10000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    lists:foreach(
        fun(_) ->
            _ = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, 100)
    ),

    ?assertMatch(
        {error, {queue_full, _}},
        hstreamdb:append_flush(producer(Config), sample_record())
    ).

t_overflooded_batches(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 1,
            max_batches => 1,
            interval => 10000
        }
    },

    ?assertWaitEvent(
        ok = start_producer(Config, ProducerOptions),
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    lists:foreach(
        fun(_) ->
            _ = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, 100)
    ),

    ?assertMatch(
        {error, {batch_count_too_large, _}},
        hstreamdb:append_flush(producer(Config), sample_record())
    ).

t_batch_reap(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 100,
            max_batches => 100,
            interval => 10000,
            batch_reap_timeout => 0
        }
    },

    ok = start_producer(Config, ProducerOptions),

    ok = hstreamdb:append(producer(Config), sample_record()),

    ok = hstreamdb:flush(producer(Config)),

    ?assertResult({error, ?ERROR_BATCH_TIMEOUT}).

t_append_flush(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            pool_size => 1,
            max_records => 1,
            max_batches => 10,
            interval => 10000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    lists:foreach(
        fun(_) ->
            ok = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, 5)
    ),

    {PKey, Record} = sample_record(),
    {ok, #{}} = hstreamdb:append_flush(producer(Config), {PKey, Record}),

    lists:foreach(
        fun(_) ->
            ?assertOkResult()
        end,
        lists:seq(1, 5)
    ).

t_append_with_errors(Config) ->
    ?assertWaitEvent(
        ok = start_producer(Config, producer_quick_recover_options()),
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    hstreamdb_test_helpers:with_failures(
        [{down, "hserver0"}, {down, "hserver1"}],
        fun() ->
            lists:foreach(
                fun(_) ->
                    ok = hstreamdb:append(producer(Config), sample_record())
                end,
                lists:seq(1, 5)
            ),
            timer:sleep(1000)
        end
    ),
    timer:sleep(2000),
    assert_ok_flush_result(5).

t_append_with_dead_writers(Config) ->
    ok = start_producer(Config, producer_quick_recover_options(1000)),

    {_, Writers} = lists:unzip(
        ecpool:workers(hstreamdb_batch_aggregator:writer_name(producer(Config)))
    ),

    lists:foreach(
        fun(WriterWPid) ->
            ecpool_worker:exec(
                WriterWPid,
                fun(WriterPid) -> exit(WriterPid, kill) end,
                5000
            )
        end,
        Writers
    ),

    lists:foreach(
        fun(_) ->
            ok = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, 5)
    ),
    %% auto_reconnect interval is 5 seconds
    timer:sleep(6000),
    assert_ok_flush_result(5).

t_append_sync_errors(Config) ->
    ?assertWaitEvent(
        ok = start_producer(Config, producer_quick_recover_options()),
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    Caller = self(),

    hstreamdb_test_helpers:with_failures(
        [{down, "hserver0"}, {down, "hserver1"}],
        fun() ->
            lists:foreach(
                fun(_) ->
                    spawn_link(fun() ->
                        Caller ! {result, hstreamdb:append_sync(producer(Config), sample_record())}
                    end)
                end,
                lists:seq(1, 5)
            ),
            timer:sleep(1000)
        end
    ),
    timer:sleep(2000),
    lists:foreach(
        fun(_) ->
            ?assertReceived({result, {ok, _}})
        end,
        lists:seq(1, 5)
    ).

t_append_with_timeouts(Config) ->
    ?assertWaitEvent(
        ok = start_producer(Config, producer_quick_recover_options()),
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    hstreamdb_test_helpers:with_failures(
        [{timeout, "hserver0"}, {timeout, "hserver1"}],
        fun() ->
            lists:foreach(
                fun(_) ->
                    ok = hstreamdb:append(producer(Config), sample_record())
                end,
                lists:seq(1, 5)
            ),
            timer:sleep(1000)
        end
    ),
    timer:sleep(2000),
    assert_ok_flush_result(5).

t_append_with_constant_errors(Config) ->
    ?assertWaitEvent(
        ok = start_producer(Config, producer_quick_recover_options()),
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    Pid = hstreamdb_test_helpers:start_disruptor(
        ["hserver0", "hserver1"],
        [down, timeout],
        1000,
        2000
    ),

    %% Append during 25 seconds, experiencing constant random tcp errors
    lists:foreach(
        fun(N) ->
            timer:sleep(5),
            ok = hstreamdb:append(producer(Config), sample_record()),
            (N rem 1000 =:= 0) andalso ct:print("appended: ~p~n", [N])
        end,
        lists:seq(1, 5000)
    ),
    timer:sleep(2000),
    hstreamdb_test_helpers:stop_disruptor(Pid),
    assert_ok_flush_result(5000).

t_append_unavailable_from_start(Config) ->
    hstreamdb_test_helpers:with_failures(
        [{down, "hserver0"}, {down, "hserver1"}],
        fun() ->
            ok = start_producer(Config, producer_quick_recover_options()),
            lists:foreach(
                fun(_) ->
                    ok = hstreamdb:append(producer(Config), sample_record())
                end,
                lists:seq(1, 5)
            ),
            timer:sleep(1000)
        end
    ),
    timer:sleep(2000),
    assert_ok_flush_result(5).

t_append_sync_unavailable_from_start(Config) ->
    Caller = self(),
    hstreamdb_test_helpers:with_failures(
        [{down, "hserver0"}, {down, "hserver1"}],
        fun() ->
            ok = start_producer(Config, producer_quick_recover_options()),

            lists:foreach(
                fun(_) ->
                    spawn_link(fun() ->
                        Caller ! {result, hstreamdb:append_sync(producer(Config), sample_record())}
                    end)
                end,
                lists:seq(1, 5)
            ),
            timer:sleep(1000)
        end
    ),
    timer:sleep(2000),
    lists:foreach(
        fun(_) ->
            ?assertReceived({result, {ok, _}})
        end,
        lists:seq(1, 5)
    ).

t_append_unexisting_stream(Config) ->
    DiscoveryOptions = #{
        backoff_options => {100, 1000, 2}
    },
    ProducerOptions = maps:put(
        discovery_options, DiscoveryOptions, producer_quick_recover_options()
    ),

    Client = ?config(client, Config),
    _ = hstreamdb_client:delete_stream(Client, ?STREAM),

    ?check_trace(
        begin
            ok = start_producer(Config, ProducerOptions),
            timer:sleep(5000)
        end,
        fun(_, Trace) ->
            Retries = [
                Event
             || #{state := {discovering, _}} = Event <- ?of_kind(
                    hstreamdb_discovery_next_state, Trace
                )
            ],
            ?assert(length(Retries) < 9),
            ?assert(length(Retries) > 5)
        end
    ).

t_append_sync(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10,
            max_batches => 10,
            interval => 500
        }
    },

    ok = start_producer(Config, ProducerOptions),

    {Time, Res} = timer:tc(
        fun() ->
            hstreamdb_batch_aggregator:append_sync(producer(Config), sample_record(), 1000)
        end
    ),
    ?assertMatch({ok, #{}}, Res),
    ?assert(Time > 500),

    ?assertEqual(
        {error, ?ERROR_TIMEOUT},
        hstreamdb_batch_aggregator:append_sync(producer(Config), sample_record(), 100)
    ).

t_stream_recreate(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10,
            max_batches => 10,
            interval => 500,
            batch_max_retries => 1
        }
    },

    Client = ?config(client, Config),

    ok = start_producer(Config, ProducerOptions),

    ?assertMatch(
        {ok, #{}},
        hstreamdb_batch_aggregator:append_sync(producer(Config), sample_record(), 1000)
    ),

    ok = snabbkaffe:cleanup(),

    _ = hstreamdb_client:delete_stream(Client, ?STREAM),

    ?assertWaitEvent(
        begin
            %% To initiate rediscovery
            {error, {unavailable, _}} = hstreamdb_batch_aggregator:append_flush(
                producer(Config), sample_record(), 100
            ),
            ok = hstreamdb_client:create_stream(Client, ?STREAM, 2, ?DAY, 5)
        end,
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    ?assertMatch(
        {ok, #{}},
        hstreamdb_batch_aggregator:append_sync(producer(Config), sample_record(), 1000)
    ).

t_graceful_stop(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 5,
        buffer_options => #{
            max_records => 1000,
            max_batches => 10,
            interval => 1000
        }
    },

    ok = start_producer(Config, ProducerOptions),

    N = 500,

    lists:foreach(
        fun(_) ->
            ok = hstreamdb:append(producer(Config), sample_record())
        end,
        lists:seq(1, N)
    ),

    ok = hstreamdb:stop_producer(producer(Config)),

    assert_ok_flush_result(N).

t_append_gzip(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            compression_type => gzip
        }
    },

    ok = start_producer(Config, ProducerOptions),

    {PKey0, Record0} = sample_record(),
    ?assertMatch(
        {ok, #{}},
        hstreamdb:append_flush(producer(Config), {PKey0, Record0})
    ),

    {PKey1, Record1} = bad_payload_record(),
    ?assertMatch(
        {error, _},
        hstreamdb:append_flush(producer(Config), {PKey1, Record1})
    ).

t_append_zstd(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            compression_type => zstd
        }
    },

    ok = start_producer(Config, ProducerOptions),

    {PKey0, Record0} = sample_record(),
    ?assertMatch(
        {ok, #{}},
        hstreamdb:append_flush(producer(Config), {PKey0, Record0})
    ),

    {PKey1, Record1} = bad_payload_record(),
    ?assertMatch(
        {error, _},
        hstreamdb:append_flush(producer(Config), {PKey1, Record1})
    ).

t_nonexistent_stream(Config) ->
    ProducerOptions = #{
        stream => "nonexistent_stream",
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10000,
            interval => 10000,
            batch_max_retries => 1
        }
    },

    ok = start_producer(Config, ProducerOptions),

    %% batch_aggregator is still discovering
    ?assertMatch(
        {error, ?ERROR_TIMEOUT},
        hstreamdb:append_sync(producer(Config), sample_record(), 1000)
    ).

t_nonexistent_stream_stop(Config) ->
    ProducerOptions = #{
        stream => "nonexistent_stream",
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10000,
            interval => 10000,
            batch_max_retries => 1
        }
    },

    ok = start_producer(Config, ProducerOptions),
    ok = hstreamdb:append(producer(Config), sample_record()),

    ok = hstreamdb:stop_producer(producer(Config)).

t_removed_stream(Config) ->
    ProducerOptions = #{
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 10,
            max_batches => 10,
            interval => 500,
            batch_max_retries => 2,
            backoff_options => {100, 200, 1.1}
        }
    },

    ?assertWaitEvent(
        ok = start_producer(Config, ProducerOptions),
        #{?snk_kind := batch_aggregator_discovered},
        5000
    ),

    Client = ?config(client, Config),
    _ = hstreamdb_client:delete_stream(Client, ?STREAM),

    ?assertMatch(
        {error, {cannot_access_shard, badarg}},
        hstreamdb:append_sync(producer(Config), sample_record(), 1000)
    ).

t_append_sync_record_timeout(Config) ->
    ProducerOptions = producer_quick_recover_options(),

    ok = start_producer(Config, ProducerOptions),
    ?assertMatch(
        {error, record_timeout},
        hstreamdb:append_sync(producer(Config), sample_record(), 1, 10000)
    ).

t_append_sync_timeout_while_success(Config) ->
    ProducerOptions = producer_quick_recover_options(),

    ok = start_producer(Config, ProducerOptions),

    ?assertWaitEvent(
        ?assertMatch(
            {error, timeout},
            hstreamdb:append_sync(producer(Config), sample_record(), 10000, 1)
        ),
        #{?snk_kind := send_response_to_record},
        5000
    ),

    ReaderOptions = #{
        mgr_client_options => hstreamdb_test_helpers:default_client_options(),
        stream => ?STREAM,
        pool_size => 5
    },

    Reader = "reader_" ++ atom_to_list(?FUNCTION_NAME),
    ok = hstreamdb:start_reader(Reader, ReaderOptions),

    % Read all records

    Limits = #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}}
    },

    Res0 = hstreamdb:read_stream_key(Reader, "PK", Limits),

    ?assertMatch(
        {ok, [#{payload := <<"payload">>}]},
        Res0
    ).

t_append_sync_unexpected_write_timeout(Config) ->
    ProducerOptions = producer_quick_recover_options(3),

    ok = start_producer(Config, ProducerOptions),

    meck:new(hstreamdb_client, [passthrough, no_history]),
    meck:expect(hstreamdb_client, append, fun(_, _, _) -> timer:sleep(999999999) end),

    ?assertMatch(
        {error, unexpected_write_timeout},
        hstreamdb:append_sync(producer(Config), sample_record(), 10000)
    ),

    meck:unload(hstreamdb_client).

t_append_sync_unexpected_write_error(Config) ->
    ProducerOptions = producer_quick_recover_options(1),

    ok = start_producer(Config, ProducerOptions),

    meck:new(hstreamdb_client, [passthrough, no_history]),
    meck:expect(hstreamdb_client, append, fun(_, _, _) -> {error, xxx} end),

    ?assertMatch(
        {error, xxx},
        hstreamdb:append_sync(producer(Config), sample_record(), 10000)
    ),

    meck:unload(hstreamdb_client).

t_append_sync_unexpected_write_exit(Config) ->
    ProducerOptions = producer_quick_recover_options(1),

    ok = start_producer(Config, ProducerOptions),

    meck:new(hstreamdb_client, [passthrough, no_history]),
    meck:expect(hstreamdb_client, append, fun(_, _, _) -> meck:exception(exit, xxx) end),

    ?assertMatch(
        {error, {unexpected_result, xxx}},
        hstreamdb:append_sync(producer(Config), sample_record(), 10000)
    ),

    meck:unload(hstreamdb_client).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

producer_quick_recover_options() ->
    producer_quick_recover_options(10).

producer_quick_recover_options(MaxRetries) ->
    ClientOptions = hstreamdb_test_helpers:client_options(#{
        grpc_timeout => 100
    }),
    #{
        client_options => ClientOptions,
        buffer_pool_size => 1,
        buffer_options => #{
            max_records => 2,
            max_batches => 1000,
            interval => 100,
            batch_max_retries => MaxRetries,
            backoff_options => {100, 200, 2},
            batch_reap_timeout => 100000
        },
        discovery_options => #{
            backoff_options => {100, 100, 2}
        },
        writer_options => #{
            grpc_timeout => 200
        }
    }.

start_producer(Config, Options) ->
    BufferOptions0 = maps:get(buffer_options, Options, #{}),
    BufferOptions = maps:merge(#{callback => callback()}, BufferOptions0),
    Options0 = #{
        client_options => hstreamdb_test_helpers:default_client_options(),
        stream => ?STREAM
    },
    Options1 = maps:merge(Options0, Options),
    Options2 = Options1#{buffer_options => BufferOptions},

    hstreamdb:start_producer(?config(producer_name, Config), Options2).

sample_record() ->
    PartitioningKey = "PK",
    PayloadType = raw,
    Payload = <<"payload">>,
    hstreamdb:to_record(PartitioningKey, PayloadType, Payload).

bad_payload_record() ->
    PartitioningKey = "PK",
    PayloadType = raw,
    Payload = payload_I_am_atom,
    hstreamdb:to_record(PartitioningKey, PayloadType, Payload).

callback() ->
    Pid = self(),
    fun(Result) ->
        Pid ! {producer_result, Result}
    end.

assert_ok_flush_result(0) ->
    ok;
assert_ok_flush_result(N) when N > 0 ->
    ?assertOkResult(),
    assert_ok_flush_result(N - 1).

producer(Config) ->
    ?config(producer_name, Config).
