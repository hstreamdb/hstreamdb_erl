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

-module(hstreamdb_read_SUITE).

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

init_per_testcase(Case, Config) ->
    Client = hstreamdb_test_helpers:client(test_c),
    StreamName =
        "stream1_" ++ integer_to_list(erlang:system_time()) ++ "_" ++
            integer_to_list(erlang:unique_integer([positive])),
    _ = hstreamdb_client:delete_stream(Client, StreamName),
    ok = hstreamdb_client:create_stream(Client, StreamName, 2, ?DAY, shard_count(Case)),
    [{client, Client}, {stream_name, StreamName} | Config].
end_per_testcase(_Case, Config) ->
    Client = ?config(client, Config),
    StreamName = ?config(stream_name, Config),
    ok = hstreamdb_client:delete_stream(Client, StreamName),
    _ = hstreamdb_client:stop(Client),
    ok.

t_read_single_shard_stream(Config) ->
    Client = ?config(client, Config),
    StreamName = ?config(stream_name, Config),

    Producer = ?FUNCTION_NAME,
    ProducerOptions = #{
        buffer_pool_size => 1,
        writer_pool_size => 1,
        stream => StreamName,
        client_options => hstreamdb_test_helpers:default_client_options(),
        buffer_options => #{
            max_records => 100,
            max_time => 10000
        }
    },
    ok = hstreamdb:start_producer(Producer, ProducerOptions),

    PartitioningKey = "PK",

    ok = lists:foreach(
        fun(N) ->
            Payload = term_to_binary({item, N}),
            Record = hstreamdb:to_record(PartitioningKey, raw, Payload),
            ok = hstreamdb:append(Producer, Record)
        end,
        lists:seq(1, 10000)
    ),

    Payload = term_to_binary({item, 10001}),
    Record = hstreamdb:to_record(PartitioningKey, raw, Payload),
    {ok, _} = hstreamdb:append_flush(Producer, Record),

    CM0 = hstreamdb:start_client_manager(Client),
    Res0 = hstreamdb:read_single_shard_stream(CM0, ?config(stream_name, Config), #{
        limits => #{
            from => #{offset => {specialOffset, 0}},
            until => #{offset => {specialOffset, 1}},
            maxReadBatches => 100000
        }
    }),

    ?assertMatch(
        {ok, _, _},
        Res0
    ),

    {ok, Recs, CM1} = Res0,

    ?assertEqual(10001, length(Recs)),

    CountsByBatchId = lists:foldl(
        fun(#{recordId := #{batchId := BatchId}}, Counts) ->
            maps:update_with(
                BatchId,
                fun(N) -> N + 1 end,
                1,
                Counts
            )
        end,
        #{},
        Recs
    ),

    ?assertEqual(
        [1 | [100 || _ <- lists:seq(1, 100)]],
        lists:sort(maps:values(CountsByBatchId))
    ),

    {value, #{recordId := MidRecordId}} = lists:search(
        fun(#{payload := P}) ->
            term_to_binary({item, 5001}) =:= P
        end,
        Recs
    ),

    MidRecordOffset = {recordOffset, MidRecordId},

    Res1 = hstreamdb:read_single_shard_stream(CM0, ?config(stream_name, Config), #{
        limits => #{
            from => #{offset => {specialOffset, 0}},
            until => #{offset => MidRecordOffset},
            maxReadBatches => 100000
        }
    }),
    Res2 = hstreamdb:read_single_shard_stream(CM0, ?config(stream_name, Config), #{
        limits => #{
            from => #{offset => MidRecordOffset},
            until => #{offset => {specialOffset, 1}},
            maxReadBatches => 100000
        }
    }),

    ?assertMatch(
        {ok, _, _},
        Res1
    ),

    ?assertMatch(
        {ok, _, _},
        Res2
    ),

    {ok, Recs1, _} = Res1,
    {ok, Recs2, _} = Res2,

    ?assertEqual(10101, length(Recs1) + length(Recs2)),

    ok = hstreamdb:stop_client_manager(CM1),
    ok = hstreamdb:stop_producer(Producer).

t_read_stream_key_with_shard_reader(Config) ->
    %% Prepare records

    Producer = ?FUNCTION_NAME,
    ProducerOptions = #{
        buffer_pool_size => 1,
        writer_pool_size => 1,
        stream => ?config(stream_name, Config),
        client_options => hstreamdb_test_helpers:default_client_options(),
        buffer_options => #{
            max_records => 10,
            max_time => 10000
        }
    },

    ok = hstreamdb:start_producer(Producer, ProducerOptions),

    ok = lists:foreach(
        fun(PartitioningKey) ->
            ok = lists:foreach(
                fun(N) ->
                    Payload = term_to_binary({item, N}),
                    Record = hstreamdb:to_record(PartitioningKey, raw, Payload),
                    ok = hstreamdb:append(Producer, Record)
                end,
                lists:seq(1, 100)
            )
        end,
        ["PK0", "PK1", "PK2", "PK3"]
    ),

    Record = hstreamdb:to_record("PK", raw, <<>>),
    {ok, _} = hstreamdb:append_flush(Producer, Record),

    %% Read records

    ReaderOptions = #{
        mgr_client_options => hstreamdb_test_helpers:default_client_options(),
        stream => ?config(stream_name, Config),
        pool_size => 5
    },

    Reader = "reader_" ++ atom_to_list(?FUNCTION_NAME),
    ok = hstreamdb:start_reader(Reader, ReaderOptions),

    Limits = #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}},
        maxReadBatches => 100000
    },

    Res0 = hstreamdb:read_stream_key_shard(Reader, "PK1", Limits),

    ?assertMatch(
        {ok, _},
        Res0
    ),

    {ok, Recs} = Res0,

    ?assertEqual(100, length(Recs)),
    assert_recs_in_order(Recs),

    ok = hstreamdb:stop_reader(Reader).

t_read_stream_key(Config) ->
    %% Prepare records

    Producer = ?FUNCTION_NAME,
    ProducerOptions = #{
        buffer_pool_size => 10,
        writer_pool_size => 20,
        stream => ?config(stream_name, Config),
        client_options => hstreamdb_test_helpers:default_client_options(),
        buffer_options => #{
            max_records => 10,
            max_time => 10000
        }
    },

    ok = hstreamdb:start_producer(Producer, ProducerOptions),

    ok = lists:foreach(
        fun(PartitioningKey) ->
            ok = lists:foreach(
                fun(N) ->
                    Payload = term_to_binary({item, N}),
                    Record = hstreamdb:to_record(PartitioningKey, raw, Payload),
                    ok = hstreamdb:append(Producer, Record)
                end,
                lists:seq(1, 999)
            )
        end,
        ["PK0", "PK1", "PK2", "PK3"]
    ),

    Record = hstreamdb:to_record("PK", raw, <<>>),
    {ok, _} = hstreamdb:append_flush(Producer, Record),

    ok = hstreamdb:stop_producer(Producer),

    %% Read records

    ReaderOptions = #{
        mgr_client_options => hstreamdb_test_helpers:default_client_options(),
        stream => ?config(stream_name, Config),
        pool_size => 5
    },

    Reader = "reader_" ++ atom_to_list(?FUNCTION_NAME),
    ok = hstreamdb:start_reader(Reader, ReaderOptions),

    % Read all records

    Limits0 = #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}}
    },

    Res0 = hstreamdb:read_stream_key(Reader, "PK1", Limits0),

    ?assertMatch(
        {ok, _},
        Res0
    ),

    {ok, Recs0} = Res0,
    ok = assert_recs_in_order(Recs0),

    ?assertEqual(999, length(Recs0)),

    % Read less then total records, but more then one read round

    Limits1 = #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}},
        readRecordCount => 950
    },

    Res1 = hstreamdb:read_stream_key(Reader, "PK1", Limits1),

    ?assertMatch(
        {ok, _},
        Res1
    ),

    {ok, Recs1} = Res1,

    ?assertEqual(950, length(Recs1)),
    ok = assert_recs_in_order(Recs1),

    % Read less then total records, and less then one read round

    Limits2 = #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}},
        readRecordCount => 121
    },

    Res2 = hstreamdb:read_stream_key(Reader, "PK1", Limits2),

    ?assertMatch(
        {ok, _},
        Res2
    ),

    {ok, Recs2} = Res2,

    ?assertEqual(121, length(Recs2)),
    ok = assert_recs_in_order(Recs2),

    ok = hstreamdb:stop_reader(Reader).

t_trim(Config) ->
    StreamName = ?config(stream_name, Config),

    %% Prepare records

    Producer = ?FUNCTION_NAME,
    ProducerOptions = #{
        buffer_pool_size => 2,
        writer_pool_size => 2,
        stream => StreamName,
        client_options => hstreamdb_test_helpers:default_client_options(),
        buffer_options => #{
            max_records => 10,
            max_time => 10
        }
    },

    ok = hstreamdb:start_producer(Producer, ProducerOptions),

    {ok, _Id0} = hstreamdb:append_sync(
        Producer,
        hstreamdb:to_record(<<"PK">>, raw, <<"R0">>)
    ),
    {ok, Id1} = hstreamdb:append_sync(
        Producer,
        hstreamdb:to_record(<<"PK">>, raw, <<"R1">>)
    ),

    ok = hstreamdb:stop_producer(Producer),

    %% Read records

    ReaderOptions = #{
        mgr_client_options => hstreamdb_test_helpers:default_client_options(),
        stream => StreamName,
        pool_size => 5
    },

    Reader = "reader_" ++ atom_to_list(?FUNCTION_NAME),
    ok = hstreamdb:start_reader(Reader, ReaderOptions),

    Limits = #{
        from => #{offset => {specialOffset, 0}},
        until => #{offset => {specialOffset, 1}}
    },

    ?assertMatch(
        {ok, [#{payload := <<"R0">>}, #{payload := <<"R1">>}]},
        hstreamdb:read_stream_key(Reader, "PK", Limits)
    ),

    {ok, _} = hstreamdb_client:trim(
        ?config(client, Config),
        StreamName,
        [Id1]
    ),

    ?assertMatch(
        {ok, [#{payload := <<"R1">>}]},
        hstreamdb:read_stream_key(Reader, "PK", Limits)
    ),

    ok = hstreamdb:stop_reader(Reader).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

assert_recs_in_order([#{payload := PayloadA}, #{payload := PayloadB} = RecB | Rest]) ->
    {item, NA} = binary_to_term(PayloadA),
    {item, NB} = binary_to_term(PayloadB),
    ?assert(NA < NB),
    assert_recs_in_order([RecB | Rest]);
assert_recs_in_order([_]) ->
    ok.

shard_count(t_read_single_shard_stream) ->
    1;
shard_count(_) ->
    2.
