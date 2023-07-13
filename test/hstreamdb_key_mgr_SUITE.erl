-module(hstreamdb_key_mgr_SUITE).

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

t_choose_shard(Config) ->
    Client = ?config(client, Config),

    _ = hstreamdb:delete_stream(Client, "stream1"),
    ok = hstreamdb:create_stream(Client, "stream1", 2, ?DAY, 5),

    %% uncached shards
    ok = test_choose_shard(Client, 0),

    %% cached shards
    ok = test_choose_shard(Client, 10000).

test_choose_shard(Client, UpdateInterval) ->
    KeyMgr0 = hstreamdb_key_mgr:start(
        [
            {client, Client},
            {stream, "stream1"},
            {shard_update_interval, UpdateInterval}
        ]
    ),

    RandomKeys = [integer_to_binary(rand:uniform(999999)) || _ <- lists:seq(1, 100)],

    {ShardIds, KeyMgr1} = lists:mapfoldl(
        fun(Key, KeyMgr) ->
            hstreamdb_key_mgr:choose_shard(Key, KeyMgr)
        end,
        KeyMgr0,
        RandomKeys
    ),

    UniqShardIds = lists:usort(ShardIds),

    ?assertEqual(
        5,
        length(UniqShardIds)
    ),

    ok = hstreamdb_key_mgr:stop(KeyMgr1).
