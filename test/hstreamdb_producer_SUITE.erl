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

-define(assertOkFlushResult(), ?assertFlushResult({ok, _})).

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

t_start_stop(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 4},
                       {stream, "stream1"},
                       {callback, callback()},
                       {max_records, 1000},
                       {interval, 1000}
                      ],

    {ok, _} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    ?assertMatch(
       {error, _},
       hstreamdb:start_producer(Client, test_producer, ProducerOptions)),

    ?assertEqual(
       ok,
       hstreamdb:stop_producer(test_producer)).

t_flush_by_timeout(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 10000},
                       {interval, 100}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    ?assertEqual(
       ok,
       hstreamdb:append(Producer, sample_record())),

    ?assertOkFlushResult(),

    ok = hstreamdb:stop_producer(Producer).


t_flush_explicit(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 10000},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    ?assertEqual(
       ok,
       hstreamdb:append(Producer, sample_record())),

    ok = hstreamdb:flush(Producer),

    ?assertOkFlushResult(),

    ok = hstreamdb:stop_producer(Producer).

t_append_flush_no_callback(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {stream, ?STREAM},
                       {callback, fun(_Result) ->
                                          ct:fail("callback should not be called for append_flush")
                                  end},
                       {max_records, 10000},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    ?assertMatch(
       {ok, _},
       hstreamdb:append_flush(Producer, sample_record())),

    ok = hstreamdb:stop_producer(Producer).

t_flush_by_limit(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 20},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),


    lists:foreach(
      fun(_) ->
        ok = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 30)),

    ok = hstreamdb:flush(Producer),

    ok = assert_ok_flush_result(20),

    ok = hstreamdb:stop_producer(Producer).

t_append_batch(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 10000},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),


    lists:foreach(
      fun(_) ->
        ok = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 100)),

    ok = hstreamdb:flush(Producer),

    ok = assert_ok_flush_result(100),

    ok = hstreamdb:stop_producer(Producer).

t_overflooded(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 1},
                       {max_batches, 1},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    lists:foreach(
      fun(_) ->
        _ = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 100)),

    ?assertMatch(
        {error,{batch_count_too_large, _}},
       hstreamdb:append_flush(Producer, sample_record())),

    ok = hstreamdb:stop_producer(Producer).

t_batch_reap(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 100},
                       {max_batches, 100},
                       {batch_reap_timeout, 0},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    ok = hstreamdb:append(Producer, sample_record()),

    ok = hstreamdb:flush(Producer),

    ?assertFlushResult({error, timeout}),

    ok = hstreamdb:stop_producer(Producer).

t_append_flush(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {max_records, 1},
                       {max_batches, 10},
                       {interval, 10000}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    lists:foreach(
      fun(_) ->
        ok = hstreamdb:append(Producer, sample_record())
      end,
      lists:seq(1, 5)),

    {PKey, Record} = sample_record(),
    {ok, _} = hstreamdb:append_flush(Producer, {PKey, Record}),

    lists:foreach(
      fun(_) ->
        ?assertOkFlushResult()
      end,
      lists:seq(1, 5)),

    ok = hstreamdb:stop_producer(Producer).

t_append_gzip(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {compression_type, gzip}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    {PKey0, Record0} = sample_record(),
    {ok, _} = hstreamdb:append_flush(Producer, {PKey0, Record0}),

    {PKey1, Record1} = bad_payload_record(),
    {error, _} = hstreamdb:append_flush(Producer, {PKey1, Record1}),

    ok = hstreamdb:stop_producer(Producer).

t_append_zstd(Config) ->
    Client = ?config(client, Config),

    ProducerOptions = [
                       {pool_size, 1},
                       {stream, ?STREAM},
                       {callback, callback()},
                       {compression_type, zstd}
                      ],

    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    {PKey0, Record0} = sample_record(),
    {ok, _} = hstreamdb:append_flush(Producer, {PKey0, Record0}),

    {PKey1, Record1} = bad_payload_record(),
    {error, _} = hstreamdb:append_flush(Producer, {PKey1, Record1}),

    ok = hstreamdb:stop_producer(Producer).

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

