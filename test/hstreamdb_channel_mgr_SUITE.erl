-module(hstreamdb_channel_mgr_SUITE).

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
    _ = hstreamdb:stop_client(Client),
    _ = application:stop(hstreamdb_erl),
    ok.

t_invalid_options(Config) ->
    Client = ?config(client, Config),

    ?assertException(
        error,
        {bad_options, _},
        hstreamdb_key_mgr:start([{client, Client}])
    ),

    ?assertException(
        error,
        {bad_options, _},
        hstreamdb_key_mgr:start([{stream, "stream1"}])
    ).

t_lookup_channel(Config) ->
    Client = ?config(client, Config),

    _ = hstreamdb:delete_stream(Client, "stream1"),
    ok = hstreamdb:create_stream(Client, "stream1", 2, ?DAY, 5),

    KeyMgr0 = hstreamdb_key_mgr:start(
        [
            {client, Client},
            {stream, "stream1"}
        ]
    ),

    ChanMgr0 = hstreamdb_channel_mgr:start(
        [
            {client, Client},
            {stream, "stream1"}
        ]
    ),

    Key = <<"key">>,

    {ShardId, KeyMgr1} = hstreamdb_key_mgr:choose_shard(Key, KeyMgr0),

    {ok, Channel0, ChanMgr1} = hstreamdb_channel_mgr:lookup_channel(ShardId, ChanMgr0),
    {ok, Channel1, ChanMgr2} = hstreamdb_channel_mgr:lookup_channel(ShardId, ChanMgr1),

    ?assertEqual(Channel0, Channel1),

    ChanMgr3 = hstreamdb_channel_mgr:bad_channel(Channel0, ChanMgr2),
    {ok, Channel2, ChanMgr4} = hstreamdb_channel_mgr:lookup_channel(ShardId, ChanMgr3),

    ?assertNotEqual(Channel1, Channel2),

    ok = hstreamdb_key_mgr:stop(KeyMgr1),
    ok = hstreamdb_channel_mgr:stop(ChanMgr4).
