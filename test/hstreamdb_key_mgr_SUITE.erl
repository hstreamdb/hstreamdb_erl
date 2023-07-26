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

t_choose_shard(Config) ->
    Client = ?config(client, Config),

    _ = hstreamdb_client:delete_stream(Client, "stream1"),
    ok = hstreamdb_client:create_stream(Client, "stream1", 2, ?DAY, 5),

    %% uncached shards
    ok = test_choose_shard(Client, 0),

    %% cached shards
    ok = test_choose_shard(Client, 10000).

test_choose_shard(Client, UpdateInterval) ->
    KeyMgr0 = hstreamdb_key_mgr:start(
        Client,
        "stream1",
        #{shard_update_interval => UpdateInterval}
    ),

    RandomKeys = [integer_to_binary(rand:uniform(999999)) || _ <- lists:seq(1, 100)],

    {ShardIds, KeyMgr1} = lists:mapfoldl(
        fun(Key, KeyMgr) ->
            {ok, ShardId, NewKeyMgr} = hstreamdb_key_mgr:choose_shard(KeyMgr, Key),
            {ShardId, NewKeyMgr}
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

t_choose_shard_error(Config) ->
    Client = ?config(client, Config),
    KeyMgr = hstreamdb_key_mgr:start(
        Client,
        "non-existant-stream1",
        #{shard_update_interval => 0}
    ),

    ?assertMatch(
        {error, {cannot_list_shards, _}},
        hstreamdb_key_mgr:choose_shard(KeyMgr, <<"key">>)
    ).


