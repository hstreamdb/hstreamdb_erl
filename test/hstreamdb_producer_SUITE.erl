-module(hstreamdb_producer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("assert.hrl").

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

    ?assertResult({error, timeout}).

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
        {error, timeout},
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
        {error, timeout},
        hstreamdb:append_sync(producer(Config), sample_record(), 1000)
    ).

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
        {error, cannot_resolve_shard_id},
        hstreamdb:append_sync(producer(Config), sample_record(), 1000)
    ).

t_producer_start_legacy(Config) ->
    ProducerName = producer(Config),

    ProducerLrgacyOptions = [
        {stream, ?STREAM},
        {callback, callback()},
        {max_records, 10},
        {max_batches, 10},
        {interval, 10},
        {grpc_timeout, 5000},
        {pool_size, 5},
        {writer_pool_size, 20}
    ],

    ?assertEqual(
        {ok, ProducerName},
        hstreamdb:start_producer(?config(client, Config), ProducerName, ProducerLrgacyOptions)
    ),

    ?assertMatch(
        {error, {already_started, _}},
        hstreamdb:start_producer(?config(client, Config), ProducerName, ProducerLrgacyOptions)
    ),

    ?assertMatch(
        {ok, #{}},
        hstreamdb_batch_aggregator:append_sync(ProducerName, sample_record(), 1000)
    ).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

start_producer(Config, Options) ->
    BufferOptions0 = maps:get(buffer_options, Options, #{}),
    BufferOptions = maps:merge(#{callback => callback()}, BufferOptions0),
    Options0 = #{
        client_options => hstreamdb_test_helpers:default_options(),
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
