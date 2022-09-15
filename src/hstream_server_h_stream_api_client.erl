%%%-------------------------------------------------------------------
%% @doc Client module for grpc service hstream.server.HStreamApi.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(hstream_server_h_stream_api_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("grpc/include/grpc.hrl").

-define(SERVICE, 'hstream.server.HStreamApi').
-define(PROTO_MODULE, 'hstreamdb_api').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType),
        #{path => Path,
          service =>?SERVICE,
          message_type => MessageType,
          marshal => ?MARSHAL(Req),
          unmarshal => ?UNMARSHAL(Resp)}).

-spec echo(hstreamdb_api:echo_request())
    -> {ok, hstreamdb_api:echo_response(), grpc:metadata()}
     | {error, term()}.
echo(Req) ->
    echo(Req, #{}, #{}).

-spec echo(hstreamdb_api:echo_request(), grpc:options())
    -> {ok, hstreamdb_api:echo_response(), grpc:metadata()}
     | {error, term()}.
echo(Req, Options) ->
    echo(Req, #{}, Options).

-spec echo(hstreamdb_api:echo_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:echo_response(), grpc:metadata()}
     | {error, term()}.
echo(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/Echo">>,
                           echo_request, echo_response, <<"hstream.server.EchoRequest">>),
                      Req, Metadata, Options).

-spec create_stream(hstreamdb_api:stream())
    -> {ok, hstreamdb_api:stream(), grpc:metadata()}
     | {error, term()}.
create_stream(Req) ->
    create_stream(Req, #{}, #{}).

-spec create_stream(hstreamdb_api:stream(), grpc:options())
    -> {ok, hstreamdb_api:stream(), grpc:metadata()}
     | {error, term()}.
create_stream(Req, Options) ->
    create_stream(Req, #{}, Options).

-spec create_stream(hstreamdb_api:stream(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:stream(), grpc:metadata()}
     | {error, term()}.
create_stream(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/CreateStream">>,
                           stream, stream, <<"hstream.server.Stream">>),
                      Req, Metadata, Options).

-spec delete_stream(hstreamdb_api:delete_stream_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_stream(Req) ->
    delete_stream(Req, #{}, #{}).

-spec delete_stream(hstreamdb_api:delete_stream_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_stream(Req, Options) ->
    delete_stream(Req, #{}, Options).

-spec delete_stream(hstreamdb_api:delete_stream_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_stream(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DeleteStream">>,
                           delete_stream_request, empty, <<"hstream.server.DeleteStreamRequest">>),
                      Req, Metadata, Options).

-spec list_streams(hstreamdb_api:list_streams_request())
    -> {ok, hstreamdb_api:list_streams_response(), grpc:metadata()}
     | {error, term()}.
list_streams(Req) ->
    list_streams(Req, #{}, #{}).

-spec list_streams(hstreamdb_api:list_streams_request(), grpc:options())
    -> {ok, hstreamdb_api:list_streams_response(), grpc:metadata()}
     | {error, term()}.
list_streams(Req, Options) ->
    list_streams(Req, #{}, Options).

-spec list_streams(hstreamdb_api:list_streams_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_streams_response(), grpc:metadata()}
     | {error, term()}.
list_streams(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListStreams">>,
                           list_streams_request, list_streams_response, <<"hstream.server.ListStreamsRequest">>),
                      Req, Metadata, Options).

-spec lookup_shard(hstreamdb_api:lookup_shard_request())
    -> {ok, hstreamdb_api:lookup_shard_response(), grpc:metadata()}
     | {error, term()}.
lookup_shard(Req) ->
    lookup_shard(Req, #{}, #{}).

-spec lookup_shard(hstreamdb_api:lookup_shard_request(), grpc:options())
    -> {ok, hstreamdb_api:lookup_shard_response(), grpc:metadata()}
     | {error, term()}.
lookup_shard(Req, Options) ->
    lookup_shard(Req, #{}, Options).

-spec lookup_shard(hstreamdb_api:lookup_shard_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:lookup_shard_response(), grpc:metadata()}
     | {error, term()}.
lookup_shard(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/LookupShard">>,
                           lookup_shard_request, lookup_shard_response, <<"hstream.server.LookupShardRequest">>),
                      Req, Metadata, Options).

-spec append(hstreamdb_api:append_request())
    -> {ok, hstreamdb_api:append_response(), grpc:metadata()}
     | {error, term()}.
append(Req) ->
    append(Req, #{}, #{}).

-spec append(hstreamdb_api:append_request(), grpc:options())
    -> {ok, hstreamdb_api:append_response(), grpc:metadata()}
     | {error, term()}.
append(Req, Options) ->
    append(Req, #{}, Options).

-spec append(hstreamdb_api:append_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:append_response(), grpc:metadata()}
     | {error, term()}.
append(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/Append">>,
                           append_request, append_response, <<"hstream.server.AppendRequest">>),
                      Req, Metadata, Options).

-spec list_shards(hstreamdb_api:list_shards_request())
    -> {ok, hstreamdb_api:list_shards_response(), grpc:metadata()}
     | {error, term()}.
list_shards(Req) ->
    list_shards(Req, #{}, #{}).

-spec list_shards(hstreamdb_api:list_shards_request(), grpc:options())
    -> {ok, hstreamdb_api:list_shards_response(), grpc:metadata()}
     | {error, term()}.
list_shards(Req, Options) ->
    list_shards(Req, #{}, Options).

-spec list_shards(hstreamdb_api:list_shards_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_shards_response(), grpc:metadata()}
     | {error, term()}.
list_shards(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListShards">>,
                           list_shards_request, list_shards_response, <<"hstream.server.ListShardsRequest">>),
                      Req, Metadata, Options).

-spec create_shard_reader(hstreamdb_api:create_shard_reader_request())
    -> {ok, hstreamdb_api:create_shard_reader_response(), grpc:metadata()}
     | {error, term()}.
create_shard_reader(Req) ->
    create_shard_reader(Req, #{}, #{}).

-spec create_shard_reader(hstreamdb_api:create_shard_reader_request(), grpc:options())
    -> {ok, hstreamdb_api:create_shard_reader_response(), grpc:metadata()}
     | {error, term()}.
create_shard_reader(Req, Options) ->
    create_shard_reader(Req, #{}, Options).

-spec create_shard_reader(hstreamdb_api:create_shard_reader_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:create_shard_reader_response(), grpc:metadata()}
     | {error, term()}.
create_shard_reader(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/CreateShardReader">>,
                           create_shard_reader_request, create_shard_reader_response, <<"hstream.server.CreateShardReaderRequest">>),
                      Req, Metadata, Options).

-spec lookup_shard_reader(hstreamdb_api:lookup_shard_reader_request())
    -> {ok, hstreamdb_api:lookup_shard_reader_response(), grpc:metadata()}
     | {error, term()}.
lookup_shard_reader(Req) ->
    lookup_shard_reader(Req, #{}, #{}).

-spec lookup_shard_reader(hstreamdb_api:lookup_shard_reader_request(), grpc:options())
    -> {ok, hstreamdb_api:lookup_shard_reader_response(), grpc:metadata()}
     | {error, term()}.
lookup_shard_reader(Req, Options) ->
    lookup_shard_reader(Req, #{}, Options).

-spec lookup_shard_reader(hstreamdb_api:lookup_shard_reader_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:lookup_shard_reader_response(), grpc:metadata()}
     | {error, term()}.
lookup_shard_reader(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/LookupShardReader">>,
                           lookup_shard_reader_request, lookup_shard_reader_response, <<"hstream.server.LookupShardReaderRequest">>),
                      Req, Metadata, Options).

-spec read_shard(hstreamdb_api:read_shard_request())
    -> {ok, hstreamdb_api:read_shard_response(), grpc:metadata()}
     | {error, term()}.
read_shard(Req) ->
    read_shard(Req, #{}, #{}).

-spec read_shard(hstreamdb_api:read_shard_request(), grpc:options())
    -> {ok, hstreamdb_api:read_shard_response(), grpc:metadata()}
     | {error, term()}.
read_shard(Req, Options) ->
    read_shard(Req, #{}, Options).

-spec read_shard(hstreamdb_api:read_shard_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:read_shard_response(), grpc:metadata()}
     | {error, term()}.
read_shard(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ReadShard">>,
                           read_shard_request, read_shard_response, <<"hstream.server.ReadShardRequest">>),
                      Req, Metadata, Options).

-spec delete_shard_reader(hstreamdb_api:delete_shard_reader_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_shard_reader(Req) ->
    delete_shard_reader(Req, #{}, #{}).

-spec delete_shard_reader(hstreamdb_api:delete_shard_reader_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_shard_reader(Req, Options) ->
    delete_shard_reader(Req, #{}, Options).

-spec delete_shard_reader(hstreamdb_api:delete_shard_reader_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_shard_reader(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DeleteShardReader">>,
                           delete_shard_reader_request, empty, <<"hstream.server.DeleteShardReaderRequest">>),
                      Req, Metadata, Options).

-spec create_subscription(hstreamdb_api:subscription())
    -> {ok, hstreamdb_api:subscription(), grpc:metadata()}
     | {error, term()}.
create_subscription(Req) ->
    create_subscription(Req, #{}, #{}).

-spec create_subscription(hstreamdb_api:subscription(), grpc:options())
    -> {ok, hstreamdb_api:subscription(), grpc:metadata()}
     | {error, term()}.
create_subscription(Req, Options) ->
    create_subscription(Req, #{}, Options).

-spec create_subscription(hstreamdb_api:subscription(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:subscription(), grpc:metadata()}
     | {error, term()}.
create_subscription(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/CreateSubscription">>,
                           subscription, subscription, <<"hstream.server.Subscription">>),
                      Req, Metadata, Options).

-spec list_subscriptions(hstreamdb_api:list_subscriptions_request())
    -> {ok, hstreamdb_api:list_subscriptions_response(), grpc:metadata()}
     | {error, term()}.
list_subscriptions(Req) ->
    list_subscriptions(Req, #{}, #{}).

-spec list_subscriptions(hstreamdb_api:list_subscriptions_request(), grpc:options())
    -> {ok, hstreamdb_api:list_subscriptions_response(), grpc:metadata()}
     | {error, term()}.
list_subscriptions(Req, Options) ->
    list_subscriptions(Req, #{}, Options).

-spec list_subscriptions(hstreamdb_api:list_subscriptions_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_subscriptions_response(), grpc:metadata()}
     | {error, term()}.
list_subscriptions(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListSubscriptions">>,
                           list_subscriptions_request, list_subscriptions_response, <<"hstream.server.ListSubscriptionsRequest">>),
                      Req, Metadata, Options).

-spec check_subscription_exist(hstreamdb_api:check_subscription_exist_request())
    -> {ok, hstreamdb_api:check_subscription_exist_response(), grpc:metadata()}
     | {error, term()}.
check_subscription_exist(Req) ->
    check_subscription_exist(Req, #{}, #{}).

-spec check_subscription_exist(hstreamdb_api:check_subscription_exist_request(), grpc:options())
    -> {ok, hstreamdb_api:check_subscription_exist_response(), grpc:metadata()}
     | {error, term()}.
check_subscription_exist(Req, Options) ->
    check_subscription_exist(Req, #{}, Options).

-spec check_subscription_exist(hstreamdb_api:check_subscription_exist_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:check_subscription_exist_response(), grpc:metadata()}
     | {error, term()}.
check_subscription_exist(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/CheckSubscriptionExist">>,
                           check_subscription_exist_request, check_subscription_exist_response, <<"hstream.server.CheckSubscriptionExistRequest">>),
                      Req, Metadata, Options).

-spec delete_subscription(hstreamdb_api:delete_subscription_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_subscription(Req) ->
    delete_subscription(Req, #{}, #{}).

-spec delete_subscription(hstreamdb_api:delete_subscription_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_subscription(Req, Options) ->
    delete_subscription(Req, #{}, Options).

-spec delete_subscription(hstreamdb_api:delete_subscription_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_subscription(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DeleteSubscription">>,
                           delete_subscription_request, empty, <<"hstream.server.DeleteSubscriptionRequest">>),
                      Req, Metadata, Options).

-spec lookup_subscription(hstreamdb_api:lookup_subscription_request())
    -> {ok, hstreamdb_api:lookup_subscription_response(), grpc:metadata()}
     | {error, term()}.
lookup_subscription(Req) ->
    lookup_subscription(Req, #{}, #{}).

-spec lookup_subscription(hstreamdb_api:lookup_subscription_request(), grpc:options())
    -> {ok, hstreamdb_api:lookup_subscription_response(), grpc:metadata()}
     | {error, term()}.
lookup_subscription(Req, Options) ->
    lookup_subscription(Req, #{}, Options).

-spec lookup_subscription(hstreamdb_api:lookup_subscription_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:lookup_subscription_response(), grpc:metadata()}
     | {error, term()}.
lookup_subscription(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/LookupSubscription">>,
                           lookup_subscription_request, lookup_subscription_response, <<"hstream.server.LookupSubscriptionRequest">>),
                      Req, Metadata, Options).

-spec streaming_fetch(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
streaming_fetch(Options) ->
    streaming_fetch(#{}, Options).

-spec streaming_fetch(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
streaming_fetch(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/hstream.server.HStreamApi/StreamingFetch">>,
                          streaming_fetch_request, streaming_fetch_response, <<"hstream.server.StreamingFetchRequest">>),
                     Metadata, Options).

-spec describe_cluster(hstreamdb_api:empty())
    -> {ok, hstreamdb_api:describe_cluster_response(), grpc:metadata()}
     | {error, term()}.
describe_cluster(Req) ->
    describe_cluster(Req, #{}, #{}).

-spec describe_cluster(hstreamdb_api:empty(), grpc:options())
    -> {ok, hstreamdb_api:describe_cluster_response(), grpc:metadata()}
     | {error, term()}.
describe_cluster(Req, Options) ->
    describe_cluster(Req, #{}, Options).

-spec describe_cluster(hstreamdb_api:empty(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:describe_cluster_response(), grpc:metadata()}
     | {error, term()}.
describe_cluster(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DescribeCluster">>,
                           empty, describe_cluster_response, <<"google.protobuf.Empty">>),
                      Req, Metadata, Options).

-spec send_admin_command(hstreamdb_api:admin_command_request())
    -> {ok, hstreamdb_api:admin_command_response(), grpc:metadata()}
     | {error, term()}.
send_admin_command(Req) ->
    send_admin_command(Req, #{}, #{}).

-spec send_admin_command(hstreamdb_api:admin_command_request(), grpc:options())
    -> {ok, hstreamdb_api:admin_command_response(), grpc:metadata()}
     | {error, term()}.
send_admin_command(Req, Options) ->
    send_admin_command(Req, #{}, Options).

-spec send_admin_command(hstreamdb_api:admin_command_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:admin_command_response(), grpc:metadata()}
     | {error, term()}.
send_admin_command(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/SendAdminCommand">>,
                           admin_command_request, admin_command_response, <<"hstream.server.AdminCommandRequest">>),
                      Req, Metadata, Options).

-spec per_stream_time_series_stats(hstreamdb_api:per_stream_time_series_stats_request())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_response(), grpc:metadata()}
     | {error, term()}.
per_stream_time_series_stats(Req) ->
    per_stream_time_series_stats(Req, #{}, #{}).

-spec per_stream_time_series_stats(hstreamdb_api:per_stream_time_series_stats_request(), grpc:options())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_response(), grpc:metadata()}
     | {error, term()}.
per_stream_time_series_stats(Req, Options) ->
    per_stream_time_series_stats(Req, #{}, Options).

-spec per_stream_time_series_stats(hstreamdb_api:per_stream_time_series_stats_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_response(), grpc:metadata()}
     | {error, term()}.
per_stream_time_series_stats(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/PerStreamTimeSeriesStats">>,
                           per_stream_time_series_stats_request, per_stream_time_series_stats_response, <<"hstream.server.PerStreamTimeSeriesStatsRequest">>),
                      Req, Metadata, Options).

-spec per_stream_time_series_stats_all(hstreamdb_api:per_stream_time_series_stats_all_request())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_all_response(), grpc:metadata()}
     | {error, term()}.
per_stream_time_series_stats_all(Req) ->
    per_stream_time_series_stats_all(Req, #{}, #{}).

-spec per_stream_time_series_stats_all(hstreamdb_api:per_stream_time_series_stats_all_request(), grpc:options())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_all_response(), grpc:metadata()}
     | {error, term()}.
per_stream_time_series_stats_all(Req, Options) ->
    per_stream_time_series_stats_all(Req, #{}, Options).

-spec per_stream_time_series_stats_all(hstreamdb_api:per_stream_time_series_stats_all_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_all_response(), grpc:metadata()}
     | {error, term()}.
per_stream_time_series_stats_all(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/PerStreamTimeSeriesStatsAll">>,
                           per_stream_time_series_stats_all_request, per_stream_time_series_stats_all_response, <<"hstream.server.PerStreamTimeSeriesStatsAllRequest">>),
                      Req, Metadata, Options).

-spec execute_push_query(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
execute_push_query(Options) ->
    execute_push_query(#{}, Options).

-spec execute_push_query(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
execute_push_query(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/hstream.server.HStreamApi/ExecutePushQuery">>,
                          command_push_query, struct, <<"hstream.server.CommandPushQuery">>),
                     Metadata, Options).

-spec execute_query(hstreamdb_api:command_query())
    -> {ok, hstreamdb_api:command_query_response(), grpc:metadata()}
     | {error, term()}.
execute_query(Req) ->
    execute_query(Req, #{}, #{}).

-spec execute_query(hstreamdb_api:command_query(), grpc:options())
    -> {ok, hstreamdb_api:command_query_response(), grpc:metadata()}
     | {error, term()}.
execute_query(Req, Options) ->
    execute_query(Req, #{}, Options).

-spec execute_query(hstreamdb_api:command_query(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:command_query_response(), grpc:metadata()}
     | {error, term()}.
execute_query(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ExecuteQuery">>,
                           command_query, command_query_response, <<"hstream.server.CommandQuery">>),
                      Req, Metadata, Options).

-spec list_queries(hstreamdb_api:list_queries_request())
    -> {ok, hstreamdb_api:list_queries_response(), grpc:metadata()}
     | {error, term()}.
list_queries(Req) ->
    list_queries(Req, #{}, #{}).

-spec list_queries(hstreamdb_api:list_queries_request(), grpc:options())
    -> {ok, hstreamdb_api:list_queries_response(), grpc:metadata()}
     | {error, term()}.
list_queries(Req, Options) ->
    list_queries(Req, #{}, Options).

-spec list_queries(hstreamdb_api:list_queries_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_queries_response(), grpc:metadata()}
     | {error, term()}.
list_queries(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListQueries">>,
                           list_queries_request, list_queries_response, <<"hstream.server.ListQueriesRequest">>),
                      Req, Metadata, Options).

-spec get_query(hstreamdb_api:get_query_request())
    -> {ok, hstreamdb_api:query(), grpc:metadata()}
     | {error, term()}.
get_query(Req) ->
    get_query(Req, #{}, #{}).

-spec get_query(hstreamdb_api:get_query_request(), grpc:options())
    -> {ok, hstreamdb_api:query(), grpc:metadata()}
     | {error, term()}.
get_query(Req, Options) ->
    get_query(Req, #{}, Options).

-spec get_query(hstreamdb_api:get_query_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:query(), grpc:metadata()}
     | {error, term()}.
get_query(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/GetQuery">>,
                           get_query_request, query, <<"hstream.server.GetQueryRequest">>),
                      Req, Metadata, Options).

-spec terminate_queries(hstreamdb_api:terminate_queries_request())
    -> {ok, hstreamdb_api:terminate_queries_response(), grpc:metadata()}
     | {error, term()}.
terminate_queries(Req) ->
    terminate_queries(Req, #{}, #{}).

-spec terminate_queries(hstreamdb_api:terminate_queries_request(), grpc:options())
    -> {ok, hstreamdb_api:terminate_queries_response(), grpc:metadata()}
     | {error, term()}.
terminate_queries(Req, Options) ->
    terminate_queries(Req, #{}, Options).

-spec terminate_queries(hstreamdb_api:terminate_queries_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:terminate_queries_response(), grpc:metadata()}
     | {error, term()}.
terminate_queries(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/TerminateQueries">>,
                           terminate_queries_request, terminate_queries_response, <<"hstream.server.TerminateQueriesRequest">>),
                      Req, Metadata, Options).

-spec delete_query(hstreamdb_api:delete_query_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_query(Req) ->
    delete_query(Req, #{}, #{}).

-spec delete_query(hstreamdb_api:delete_query_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_query(Req, Options) ->
    delete_query(Req, #{}, Options).

-spec delete_query(hstreamdb_api:delete_query_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_query(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DeleteQuery">>,
                           delete_query_request, empty, <<"hstream.server.DeleteQueryRequest">>),
                      Req, Metadata, Options).

-spec restart_query(hstreamdb_api:restart_query_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
restart_query(Req) ->
    restart_query(Req, #{}, #{}).

-spec restart_query(hstreamdb_api:restart_query_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
restart_query(Req, Options) ->
    restart_query(Req, #{}, Options).

-spec restart_query(hstreamdb_api:restart_query_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
restart_query(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/RestartQuery">>,
                           restart_query_request, empty, <<"hstream.server.RestartQueryRequest">>),
                      Req, Metadata, Options).

-spec create_connector(hstreamdb_api:create_connector_request())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, term()}.
create_connector(Req) ->
    create_connector(Req, #{}, #{}).

-spec create_connector(hstreamdb_api:create_connector_request(), grpc:options())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, term()}.
create_connector(Req, Options) ->
    create_connector(Req, #{}, Options).

-spec create_connector(hstreamdb_api:create_connector_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, term()}.
create_connector(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/CreateConnector">>,
                           create_connector_request, connector, <<"hstream.server.CreateConnectorRequest">>),
                      Req, Metadata, Options).

-spec list_connectors(hstreamdb_api:list_connectors_request())
    -> {ok, hstreamdb_api:list_connectors_response(), grpc:metadata()}
     | {error, term()}.
list_connectors(Req) ->
    list_connectors(Req, #{}, #{}).

-spec list_connectors(hstreamdb_api:list_connectors_request(), grpc:options())
    -> {ok, hstreamdb_api:list_connectors_response(), grpc:metadata()}
     | {error, term()}.
list_connectors(Req, Options) ->
    list_connectors(Req, #{}, Options).

-spec list_connectors(hstreamdb_api:list_connectors_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_connectors_response(), grpc:metadata()}
     | {error, term()}.
list_connectors(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListConnectors">>,
                           list_connectors_request, list_connectors_response, <<"hstream.server.ListConnectorsRequest">>),
                      Req, Metadata, Options).

-spec get_connector(hstreamdb_api:get_connector_request())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, term()}.
get_connector(Req) ->
    get_connector(Req, #{}, #{}).

-spec get_connector(hstreamdb_api:get_connector_request(), grpc:options())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, term()}.
get_connector(Req, Options) ->
    get_connector(Req, #{}, Options).

-spec get_connector(hstreamdb_api:get_connector_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, term()}.
get_connector(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/GetConnector">>,
                           get_connector_request, connector, <<"hstream.server.GetConnectorRequest">>),
                      Req, Metadata, Options).

-spec delete_connector(hstreamdb_api:delete_connector_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_connector(Req) ->
    delete_connector(Req, #{}, #{}).

-spec delete_connector(hstreamdb_api:delete_connector_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_connector(Req, Options) ->
    delete_connector(Req, #{}, Options).

-spec delete_connector(hstreamdb_api:delete_connector_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_connector(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DeleteConnector">>,
                           delete_connector_request, empty, <<"hstream.server.DeleteConnectorRequest">>),
                      Req, Metadata, Options).

-spec pause_connector(hstreamdb_api:pause_connector_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
pause_connector(Req) ->
    pause_connector(Req, #{}, #{}).

-spec pause_connector(hstreamdb_api:pause_connector_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
pause_connector(Req, Options) ->
    pause_connector(Req, #{}, Options).

-spec pause_connector(hstreamdb_api:pause_connector_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
pause_connector(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/PauseConnector">>,
                           pause_connector_request, empty, <<"hstream.server.PauseConnectorRequest">>),
                      Req, Metadata, Options).

-spec resume_connector(hstreamdb_api:resume_connector_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
resume_connector(Req) ->
    resume_connector(Req, #{}, #{}).

-spec resume_connector(hstreamdb_api:resume_connector_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
resume_connector(Req, Options) ->
    resume_connector(Req, #{}, Options).

-spec resume_connector(hstreamdb_api:resume_connector_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
resume_connector(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ResumeConnector">>,
                           resume_connector_request, empty, <<"hstream.server.ResumeConnectorRequest">>),
                      Req, Metadata, Options).

-spec lookup_connector(hstreamdb_api:lookup_connector_request())
    -> {ok, hstreamdb_api:lookup_connector_response(), grpc:metadata()}
     | {error, term()}.
lookup_connector(Req) ->
    lookup_connector(Req, #{}, #{}).

-spec lookup_connector(hstreamdb_api:lookup_connector_request(), grpc:options())
    -> {ok, hstreamdb_api:lookup_connector_response(), grpc:metadata()}
     | {error, term()}.
lookup_connector(Req, Options) ->
    lookup_connector(Req, #{}, Options).

-spec lookup_connector(hstreamdb_api:lookup_connector_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:lookup_connector_response(), grpc:metadata()}
     | {error, term()}.
lookup_connector(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/LookupConnector">>,
                           lookup_connector_request, lookup_connector_response, <<"hstream.server.LookupConnectorRequest">>),
                      Req, Metadata, Options).

-spec list_views(hstreamdb_api:list_views_request())
    -> {ok, hstreamdb_api:list_views_response(), grpc:metadata()}
     | {error, term()}.
list_views(Req) ->
    list_views(Req, #{}, #{}).

-spec list_views(hstreamdb_api:list_views_request(), grpc:options())
    -> {ok, hstreamdb_api:list_views_response(), grpc:metadata()}
     | {error, term()}.
list_views(Req, Options) ->
    list_views(Req, #{}, Options).

-spec list_views(hstreamdb_api:list_views_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_views_response(), grpc:metadata()}
     | {error, term()}.
list_views(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListViews">>,
                           list_views_request, list_views_response, <<"hstream.server.ListViewsRequest">>),
                      Req, Metadata, Options).

-spec get_view(hstreamdb_api:get_view_request())
    -> {ok, hstreamdb_api:view(), grpc:metadata()}
     | {error, term()}.
get_view(Req) ->
    get_view(Req, #{}, #{}).

-spec get_view(hstreamdb_api:get_view_request(), grpc:options())
    -> {ok, hstreamdb_api:view(), grpc:metadata()}
     | {error, term()}.
get_view(Req, Options) ->
    get_view(Req, #{}, Options).

-spec get_view(hstreamdb_api:get_view_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:view(), grpc:metadata()}
     | {error, term()}.
get_view(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/GetView">>,
                           get_view_request, view, <<"hstream.server.GetViewRequest">>),
                      Req, Metadata, Options).

-spec delete_view(hstreamdb_api:delete_view_request())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_view(Req) ->
    delete_view(Req, #{}, #{}).

-spec delete_view(hstreamdb_api:delete_view_request(), grpc:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_view(Req, Options) ->
    delete_view(Req, #{}, Options).

-spec delete_view(hstreamdb_api:delete_view_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, term()}.
delete_view(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/DeleteView">>,
                           delete_view_request, empty, <<"hstream.server.DeleteViewRequest">>),
                      Req, Metadata, Options).

-spec list_nodes(hstreamdb_api:list_nodes_request())
    -> {ok, hstreamdb_api:list_nodes_response(), grpc:metadata()}
     | {error, term()}.
list_nodes(Req) ->
    list_nodes(Req, #{}, #{}).

-spec list_nodes(hstreamdb_api:list_nodes_request(), grpc:options())
    -> {ok, hstreamdb_api:list_nodes_response(), grpc:metadata()}
     | {error, term()}.
list_nodes(Req, Options) ->
    list_nodes(Req, #{}, Options).

-spec list_nodes(hstreamdb_api:list_nodes_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:list_nodes_response(), grpc:metadata()}
     | {error, term()}.
list_nodes(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/ListNodes">>,
                           list_nodes_request, list_nodes_response, <<"hstream.server.ListNodesRequest">>),
                      Req, Metadata, Options).

-spec get_node(hstreamdb_api:get_node_request())
    -> {ok, hstreamdb_api:node(), grpc:metadata()}
     | {error, term()}.
get_node(Req) ->
    get_node(Req, #{}, #{}).

-spec get_node(hstreamdb_api:get_node_request(), grpc:options())
    -> {ok, hstreamdb_api:node(), grpc:metadata()}
     | {error, term()}.
get_node(Req, Options) ->
    get_node(Req, #{}, Options).

-spec get_node(hstreamdb_api:get_node_request(), grpc:metadata(), grpc_client:options())
    -> {ok, hstreamdb_api:node(), grpc:metadata()}
     | {error, term()}.
get_node(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/hstream.server.HStreamApi/GetNode">>,
                           get_node_request, node, <<"hstream.server.GetNodeRequest">>),
                      Req, Metadata, Options).

