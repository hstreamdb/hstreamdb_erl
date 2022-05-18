-module(hstreamdb_erlang_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
% -include_lib("common_test/include/ct.hrl").

% init_per_testcase(_, _) ->
%     io:format(standard_error, "~s~n", [
%         os:cmd(
%             "../../../../script/prepare-test-env.sh"
%         )
%     ]),
%     [].

% end_per_testcase(_, _) ->
%     io:format(standard_error, "~s~n", [
%         os:cmd(
%             "../../../../script/cleanup-test-env.sh"
%         )
%     ]),
%     [].

all() ->
    [{group, basic}].

groups() ->
    [{basic, [t_consumer]}].

t_consumer(_) ->
    ServerUrl = "http://127.0.0.1:6570",
    StreamName = hstreamdb_erlang_utils:string_format("~s-~p-~p", [
        "___test___", erlang:unique_integer(), erlang:system_time()
    ]),
    BatchSetting = hstreamdb_erlang_producer:build_batch_setting({record_count_limit, 3}),

    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    _ = hstreamdb_erlang:delete_stream(Channel, StreamName, #{
        ignoreNonExist => true,
        force => true
    }),
    ReplicationFactor = 3,
    BacklogDuration = 60 * 30,
    ok = hstreamdb_erlang:create_stream(
        Channel, StreamName, ReplicationFactor, BacklogDuration
    ),

    StartArgs = #{
        producer_option => hstreamdb_erlang_producer:build_producer_option(
            ServerUrl, StreamName, self(), 16, BatchSetting
        )
    },
    {ok, Producer} = hstreamdb_erlang_producer:start_link(StartArgs),

    SubscriptionId = hstreamdb_erlang_utils:string_format("~s-~p-~p", [
        "___test___", erlang:unique_integer(), erlang:system_time()
    ]),
    ConsumerName = hstreamdb_erlang_utils:string_format("~s-~p-~p", [
        "___test___", erlang:unique_integer(), erlang:system_time()
    ]),

    ok = hstreamdb_erlang:create_subscription(Channel, SubscriptionId, StreamName),

    lists:foreach(
        fun(_) ->
            Record = hstreamdb_erlang_producer:build_record(
                <<"_", (erlang:integer_to_binary(erlang:unique_integer()))/binary, "_">>
            ),
            hstreamdb_erlang_producer:append(Producer, Record)
        end,
        lists:seq(1, 1000)
    ),

    SelfPid = self(),

    ConsumerFun = fun(
        ReceivedRecord, Ack
    ) ->
        io:format("~p~n", [ReceivedRecord]),
        ok = Ack(),
        SelfPid ! consumer_ok
    end,

    spawn(fun() ->
        hstreamdb_erlang_producer:flush(Producer),
        hstreamdb_erlang_consumer:start(
            ServerUrl, SubscriptionId, ConsumerName, ConsumerFun
        )
    end),

    Expr = lists:map(
        fun(_) ->
            receive
                consumer_ok -> ok
            after 2000 -> timeout
            end
        end,
        lists:seq(1, 1000)
    ),

    ?assert(
        lists:all(fun(X) -> X == ok end, Expr)
    ).
