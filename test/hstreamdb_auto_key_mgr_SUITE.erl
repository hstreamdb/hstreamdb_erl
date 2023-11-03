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

-module(hstreamdb_auto_key_mgr_SUITE).

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
    KeyMgr0 = hstreamdb_auto_key_mgr:start(
        Client,
        "stream1",
        #{shard_update_interval => UpdateInterval}
    ),

    RandomKeys = [integer_to_binary(rand:uniform(999999)) || _ <- lists:seq(1, 100)],

    {ShardIds, KeyMgr1} = lists:mapfoldl(
        fun(Key, KeyMgr) ->
            {ok, ShardId, NewKeyMgr} = hstreamdb_auto_key_mgr:choose_shard(KeyMgr, Key),
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

    ok = hstreamdb_auto_key_mgr:stop(KeyMgr1).

t_choose_shard_error(Config) ->
    Client = ?config(client, Config),
    KeyMgr = hstreamdb_auto_key_mgr:start(
        Client,
        "non-existant-stream1",
        #{shard_update_interval => 0}
    ),

    ?assertMatch(
        {error, {cannot_list_shards, _}},
        hstreamdb_auto_key_mgr:choose_shard(KeyMgr, <<"key">>)
    ).

t_choose_shard_bsearch_error(Config) ->
    Client = ?config(client, Config),
    KeyMgr0 = hstreamdb_auto_key_mgr:start(
        Client,
        "stream",
        #{shard_update_interval => 1000000}
    ),

    IntHash = int_hash(<<"7">>),
    Shards = [make_shard_info(IntHash, IntHash) || _ <- lists:seq(1, 100)],

    KeyMgr1 = hstreamdb_auto_key_mgr:set_shards(KeyMgr0, Shards),

    ?assertMatch(
        {ok, _, _},
        hstreamdb_auto_key_mgr:choose_shard(KeyMgr1, <<"7">>)
    ),

    %% We have int_hash(<<"6">>) < int_hash(<<"7">>) < int_hash(<<"3">>)

    ?assertEqual(
        {error, {cannot_find_shard, <<"6">>}},
        hstreamdb_auto_key_mgr:choose_shard(KeyMgr1, <<"6">>)
    ),

    ?assertEqual(
        {error, {cannot_find_shard, <<"3">>}},
        hstreamdb_auto_key_mgr:choose_shard(KeyMgr1, <<"3">>)
    ),

    ok = hstreamdb_auto_key_mgr:stop(KeyMgr1).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

int_hash(Bin) ->
    <<IntHash:128/big-unsigned-integer>> = crypto:hash(md5, Bin),
    IntHash.

make_shard_info(From, To) ->
    #{
        startHashRangeKey => integer_to_binary(From),
        endHashRangeKey => integer_to_binary(To),
        epoch => 5,
        isActive => true,
        shardId => erlang:unique_integer([positive]),
        streamName => <<"stream">>
    }.
