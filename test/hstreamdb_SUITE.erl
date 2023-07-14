-module(hstreamdb_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DAY, (24 * 60 * 60)).

all() ->
    hstreamdb_test_helpers:test_cases(?MODULE).

init_per_suite(Config) ->
    _ = application:ensure_all_started(hstreamdb_erl),
    Client = hstreamdb_test_helpers:client(test_c),
    [{client, Client} | Config].
end_per_suite(Config) ->
    Client = ?config(client, Config),
    _ = hstreamdb_client:stop(Client),
    _ = application:stop(hstreamdb_erl),
    ok.

t_stop_client_by_name(_Config) ->
    _ = hstreamdb_test_helpers:client(test_c1),

    ?assertEqual(
        ok,
        hstreamdb_client:stop(test_c1)
    ).

t_start_client_error(_Config) ->
    ClientOptions = hstreamdb_test_helpers:default_options(),

    ?assertMatch(
        {error, _},
        hstreamdb_client:start(test_c2, lists:keyreplace(url, 1, ClientOptions, {url, "#badurl#"}))
    ),

    ?assertMatch(
        {error, _},
        hstreamdb_client:start(
            test_c2, lists:keyreplace(url, 1, ClientOptions, {url, "http://#badurl#"})
        )
    ).

t_echo(Config) ->
    Client = ?config(client, Config),
    ?assertEqual(
        ok,
        hstreamdb_client:echo(Client)
    ).

t_create_delete_stream(Config) ->
    Client = ?config(client, Config),

    _ = hstreamdb_client:delete_stream(Client, "stream1"),

    ?assertEqual(
        ok,
        hstreamdb_client:create_stream(Client, "stream1", 2, ?DAY, 5)
    ),

    ?assertMatch(
        {error, {already_exists, _}},
        hstreamdb_client:create_stream(Client, "stream1", 2, ?DAY, 5)
    ),

    _ = hstreamdb_client:delete_stream(Client, "stream1"),

    ?assertEqual(
        ok,
        hstreamdb_client:create_stream(Client, "stream1", 2, ?DAY, 5)
    ),

    _ = hstreamdb_client:delete_stream(Client, "stream1").

t_start_stop_producer(Config) ->
    Client = ?config(client, Config),

    _ = hstreamdb_client:create_stream(Client, "stream2", 2, ?DAY, 5),

    ProducerOptions = [
        {pool_size, 4},
        {stream, "stream2"},
        {callback, fun(_) -> ok end},
        {max_records, 1000},
        {interval, 1000}
    ],
    ok = hstreamdb:start_producer(Client, test_producer, ProducerOptions),

    ?assertEqual(
        ok,
        hstreamdb:stop_producer(test_producer)
    ),

    ok = hstreamdb_client:delete_stream(Client, "stream2").

%% no op
t_start_stop_consumer(Config) ->
    Client = ?config(client, Config),

    {ok, Consumer} = hstreamdb:start_consumer(Client, test_consumer, []),

    ?assertEqual(
        ok,
        hstreamdb:stop_consumer(Consumer)
    ).
