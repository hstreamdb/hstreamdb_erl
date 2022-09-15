%%%-------------------------------------------------------------------
%% @doc Behaviour to implement for grpc service hstream.server.HStreamApi.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(hstream_server_h_stream_api_bhvr).

-callback echo(hstreamdb_api:echo_request(), grpc:metadata())
    -> {ok, hstreamdb_api:echo_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback create_stream(hstreamdb_api:stream(), grpc:metadata())
    -> {ok, hstreamdb_api:stream(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback delete_stream(hstreamdb_api:delete_stream_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_streams(hstreamdb_api:list_streams_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_streams_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback lookup_shard(hstreamdb_api:lookup_shard_request(), grpc:metadata())
    -> {ok, hstreamdb_api:lookup_shard_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback append(hstreamdb_api:append_request(), grpc:metadata())
    -> {ok, hstreamdb_api:append_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_shards(hstreamdb_api:list_shards_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_shards_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback create_shard_reader(hstreamdb_api:create_shard_reader_request(), grpc:metadata())
    -> {ok, hstreamdb_api:create_shard_reader_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback lookup_shard_reader(hstreamdb_api:lookup_shard_reader_request(), grpc:metadata())
    -> {ok, hstreamdb_api:lookup_shard_reader_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback read_shard(hstreamdb_api:read_shard_request(), grpc:metadata())
    -> {ok, hstreamdb_api:read_shard_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback delete_shard_reader(hstreamdb_api:delete_shard_reader_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback create_subscription(hstreamdb_api:subscription(), grpc:metadata())
    -> {ok, hstreamdb_api:subscription(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_subscriptions(hstreamdb_api:list_subscriptions_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_subscriptions_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback check_subscription_exist(hstreamdb_api:check_subscription_exist_request(), grpc:metadata())
    -> {ok, hstreamdb_api:check_subscription_exist_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback delete_subscription(hstreamdb_api:delete_subscription_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback lookup_subscription(hstreamdb_api:lookup_subscription_request(), grpc:metadata())
    -> {ok, hstreamdb_api:lookup_subscription_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback streaming_fetch(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

-callback describe_cluster(hstreamdb_api:empty(), grpc:metadata())
    -> {ok, hstreamdb_api:describe_cluster_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback send_admin_command(hstreamdb_api:admin_command_request(), grpc:metadata())
    -> {ok, hstreamdb_api:admin_command_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback per_stream_time_series_stats(hstreamdb_api:per_stream_time_series_stats_request(), grpc:metadata())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback per_stream_time_series_stats_all(hstreamdb_api:per_stream_time_series_stats_all_request(), grpc:metadata())
    -> {ok, hstreamdb_api:per_stream_time_series_stats_all_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback execute_push_query(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

-callback execute_query(hstreamdb_api:command_query(), grpc:metadata())
    -> {ok, hstreamdb_api:command_query_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_queries(hstreamdb_api:list_queries_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_queries_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback get_query(hstreamdb_api:get_query_request(), grpc:metadata())
    -> {ok, hstreamdb_api:query(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback terminate_queries(hstreamdb_api:terminate_queries_request(), grpc:metadata())
    -> {ok, hstreamdb_api:terminate_queries_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback delete_query(hstreamdb_api:delete_query_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback restart_query(hstreamdb_api:restart_query_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback create_connector(hstreamdb_api:create_connector_request(), grpc:metadata())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_connectors(hstreamdb_api:list_connectors_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_connectors_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback get_connector(hstreamdb_api:get_connector_request(), grpc:metadata())
    -> {ok, hstreamdb_api:connector(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback delete_connector(hstreamdb_api:delete_connector_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback pause_connector(hstreamdb_api:pause_connector_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback resume_connector(hstreamdb_api:resume_connector_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback lookup_connector(hstreamdb_api:lookup_connector_request(), grpc:metadata())
    -> {ok, hstreamdb_api:lookup_connector_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_views(hstreamdb_api:list_views_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_views_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback get_view(hstreamdb_api:get_view_request(), grpc:metadata())
    -> {ok, hstreamdb_api:view(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback delete_view(hstreamdb_api:delete_view_request(), grpc:metadata())
    -> {ok, hstreamdb_api:empty(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback list_nodes(hstreamdb_api:list_nodes_request(), grpc:metadata())
    -> {ok, hstreamdb_api:list_nodes_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback get_node(hstreamdb_api:get_node_request(), grpc:metadata())
    -> {ok, hstreamdb_api:node(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

