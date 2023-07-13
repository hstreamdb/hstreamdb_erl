-module(hstreamdb_producer_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DAY, (24 * 60 * 60)).

-define(STREAM, "stream2").

-define(assertFlushResult(Result),
        begin
            receive
                {producer_result, {{flush, ?STREAM, 1}, Result}} ->
                    ok
            after 300 ->
                      ct:fail("producer result not received")
            end
        end).

-define(assertOkFlushResult(), ?assertFlushResult(ok)).

all() ->
    hstreamdb_test_helpers:test_cases(?MODULE).

init_per_suite(Config) ->
    _ = application:ensure_all_started(hstreamdb_erl),
    Client = hstreamdb_test_helpers:client(test_c),
    _ = hstreamdb:delete_stream(Client, ?STREAM),
    ok = hstreamdb:create_stream(Client, ?STREAM, 2, ?DAY, 5),
    [{client, Client} | Config].
end_per_suite(Config) ->
    Client = ?config(client, Config),
    _ = hstreamdb:delete_stream(Client, ?STREAM),
    _ = hstreamdb:stop_client(Client),
    _ = application:stop(hstreamdb_erl),
    ok.

init_per_testcase(_TestCase, Config) ->
    [{producer_name, test_producer} | Config].
end_per_testcase(_TestCase, Config) ->
    catch hstreamdb:stop_producer(?config(producer_name, Config)),
    ok.

t_start_stop(Config) ->
    ProducerOptions = [
                       {pool_size, 4},
                       {stream, "stream1"},
                       {callback, callback()},
                       {max_records, 1000},
                       {interval, 1000}
                      ],

    {ok, _} = start_producer(Config, ProducerOptions),

    ?assertMatch(
       {error, _},
       start_producer(Config, ProducerOptions)),

    ?assertEqual(
       ok,
       hstreamdb:stop_producer(?config(producer_name, Config))).

t_flush_by_timeout(Config) ->
    ProducerOptions = [
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 10000},
                       {interval, 100}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    ?assertEqual(
       ok,
       hstreamdb:append(Producer, sample_record())),

    ?assertOkFlushResult().

t_flush_explicit(Config) ->
    ProducerOptions = [
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 10000},
                       {interval, 10000}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    ?assertEqual(
       ok,
       hstreamdb:append(Producer, sample_record())),

    ok = hstreamdb:flush(Producer),

    ?assertOkFlushResult().

t_append_flush_no_callback(Config) ->
    ProducerOptions = [
                       {stream, ?STREAM},
                       {callback, fun(_Result) ->
                                          ct:fail("callback should not be called for append_flush")
                                  end},
                       {max_records, 10000},
                       {interval, 10000}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    ?assertEqual(
       ok,
       hstreamdb:append_flush(Producer, sample_record())).

t_flush_by_limit(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 20},
                       {interval, 10000}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    lists:foreach(
      fun(_) ->
        ok = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 30)),

    ok = hstreamdb:flush(Producer),

    ok = assert_ok_flush_result(20).


t_append_batch(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 10000},
                       {interval, 10000}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    lists:foreach(
      fun(_) ->
        ok = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 100)),

    ok = hstreamdb:flush(Producer),

    ok = assert_ok_flush_result(100),

    ok = hstreamdb:stop_producer(Producer).

t_overflooded(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 1},
                       {max_batches, 1},
                       {interval, 10000}
                      ],

                      {ok, Producer} = start_producer(Config, ProducerOptions),

    lists:foreach(
      fun(_) ->
        _ = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 100)),

    ?assertMatch(
        {error,{batch_count_too_large, _}},
       hstreamdb:append_flush(Producer, sample_record())).


t_batch_reap(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 100},
                       {max_batches, 100},
                       {batch_reap_timeout, 0},
                       {interval, 10000}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    ok = hstreamdb:append(Producer, sample_record()),

    ok = hstreamdb:flush(Producer),

    ?assertFlushResult({error, timeout}).

t_append_flush(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 1},
                       {max_batches, 10},
                       {interval, 10000}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    lists:foreach(
      fun(_) ->
        ok = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 5)),

    {PKey, Record} = sample_record(),
    ok = hstreamdb:append_flush(Producer, {PKey, Record}),

    lists:foreach(
      fun(_) ->
        ?assertOkFlushResult()
      end,
      lists:seq(1, 5)).

t_append_gzip(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {compression_type, gzip}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    {PKey0, Record0} = sample_record(),
    ?assertMatch(
        ok,
        hstreamdb:append_flush(Producer, {PKey0, Record0})
    ),

    {PKey1, Record1} = bad_payload_record(),
    ?assertMatch(
        {error, _},
        hstreamdb:append_flush(Producer, {PKey1, Record1})
    ).

t_append_zstd(Config) ->
    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {compression_type, zstd}
                      ],

    {ok, Producer} = start_producer(Config, ProducerOptions),

    {PKey0, Record0} = sample_record(),
    ?assertMatch(
        ok,
        hstreamdb:append_flush(Producer, {PKey0, Record0})
    ),

    {PKey1, Record1} = bad_payload_record(),
    ?assertMatch(
        {error, _},
        hstreamdb:append_flush(Producer, {PKey1, Record1})
    ).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

start_producer(Config, Options) ->
    Client = ?config(client, Config),
    hstreamdb:start_producer(Client, ?config(producer_name, Config), Options).

sample_record() ->
    PartitioningKey = "PK",
    PayloadType = raw,
    Payload = <<"payload">>,
    hstreamdb:to_record(PartitioningKey, PayloadType, Payload).

bad_payload_record() ->
    PartitioningKey = "PK",
    PayloadType = raw,
    Payload = payload,
    hstreamdb:to_record(PartitioningKey, PayloadType, Payload).

callback() ->
    Pid = self(),
    fun(Result) ->
            Pid ! {producer_result, Result}
    end.

assert_ok_flush_result(0) ->
    ok;
assert_ok_flush_result(N) when N > 0 ->
    ?assertOkFlushResult(),
    assert_ok_flush_result(N - 1).

