-module(hstreamdb_shard_client_mgr_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(DAY, (24 * 60 * 60)).

all() ->
    hstreamdb_test_helpers:test_cases(?MODULE).

init_per_suite(Config) ->
    _ = application:ensure_all_started(hstreamdb_erl),
    Config.
end_per_suite(_Config) ->
    _ = application:stop(hstreamdb_erl),
    ok.

init_per_testcase(_Case, Config) ->
    Client = hstreamdb_test_helpers:client(test_c),
    [{client, Client} | Config].
end_per_testcase(_Case, Config) ->
    Client = ?config(client, Config),
    _ = hstreamdb_client:stop(Client),
    ok.

t_lookup_client(Config) ->
    Client = ?config(client, Config),

    _ = hstreamdb_client:delete_stream(Client, "stream1"),
    ok = hstreamdb_client:create_stream(Client, "stream1", 2, ?DAY, 5),

    KeyMgr0 = hstreamdb_key_mgr:start(Client, "stream1"),

    ShardClientMgr0 = hstreamdb_shard_client_mgr:start(Client),

    Key = <<"key">>,

    {ok, ShardId, KeyMgr1} = hstreamdb_key_mgr:choose_shard(KeyMgr0, Key),

    {ok, ShardClient0, ShardClientMgr1} = hstreamdb_shard_client_mgr:lookup_client(
        ShardClientMgr0, ShardId
    ),
    {ok, ShardClient1, ShardClientMgr2} = hstreamdb_shard_client_mgr:lookup_client(
        ShardClientMgr1, ShardId
    ),

    ?assertEqual(ShardClient0, ShardClient1),

    ShardClientMgr3 = hstreamdb_shard_client_mgr:bad_shart_client(ShardClientMgr2, ShardClient1),
    {ok, ShardClient2, ShardClientMgr4} = hstreamdb_shard_client_mgr:lookup_client(
        ShardClientMgr3, ShardId
    ),

    ?assertNotEqual(ShardClient1, ShardClient2),

    ok = hstreamdb_key_mgr:stop(KeyMgr1),
    ok = hstreamdb_shard_client_mgr:stop(ShardClientMgr4).
