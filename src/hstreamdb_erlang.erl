-module(hstreamdb_erlang).

-export([
    start_client_channel/1, start_client_channel/2,
    stop_client_channel/1,
    with_client_channel/2, with_client_channel/3
]).

-export([list_streams/1, create_stream/4, delete_stream/2, delete_stream/3]).

-export([list_subscriptions/1, create_subscription/3]).

-export([server_node_to_host_port/1, server_node_to_host_port/2]).
-export([lookup_stream/3]).

% --------------------------------------------------------------------------------

server_node_to_host_port(ServerNode) ->
    Host = maps:get(host, ServerNode),
    Port = maps:get(port, ServerNode),
    hstreamdb_erlang_utils:string_format("~s:~p", [Host, Port]).

server_node_to_host_port(ServerNode, Protocol) ->
    hstreamdb_erlang_utils:string_format("~p://~s", [Protocol, server_node_to_host_port(ServerNode)]).

% --------------------------------------------------------------------------------
start_client_channel(ServerUrl) ->
    start_client_channel(ServerUrl, #{}).

start_client_channel(ServerUrl, Opts) ->
    ChannelName = "hstream_client_channel-" ++ hstreamdb_erlang_utils:uid(),
    case grpc_client_sup:create_channel_pool(ChannelName, ServerUrl, Opts) of
        {error, _} = E -> E;
        _ -> {ok, ChannelName}
    end.

stop_client_channel(Channel) -> grpc_client_sup:stop_channel_pool(Channel).

with_client_channel(ServerUrl, Fun) ->
    with_client_channel(ServerUrl, #{}, Fun).

with_client_channel(ServerUrl, Opts, Fun) ->
    {Channel, Ret} =
        case start_client_channel(ServerUrl, Opts) of
            {error, Reason} -> hstreamdb_erlang_utils:throw_hstreamdb_exception(Reason);
            {ok, Ch} -> {Ch, Fun(Ch)}
        end,
    _ = stop_client_channel(Channel),
    Ret.

% --------------------------------------------------------------------------------

list_streams(Channel) ->
    Ret = hstream_server_h_stream_api_client:list_streams(
        #{},
        #{channel => Channel}
    ),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(streams, Resp)};
        R ->
            R
    end.

create_stream(Channel, StreamName, ReplicationFactor, BacklogDuration) ->
    Ret = hstream_server_h_stream_api_client:create_stream(
        #{
            streamName => StreamName,
            replicationFactor => ReplicationFactor,
            backlogDuration => BacklogDuration
        },
        #{channel => Channel}
    ),
    case Ret of
        {ok, _, _} -> ok;
        R -> R
    end.

delete_stream(Channel, StreamName) -> delete_stream(Channel, StreamName, #{}).

delete_stream(Channel, StreamName, Opts) ->
    IgnoreNonExist = maps:get(ignoreNonExist, Opts, false),
    Force = maps:get(force, Opts, false),

    Ret = hstream_server_h_stream_api_client:delete_stream(
        #{
            streamName => StreamName,
            ignoreNonExist => IgnoreNonExist,
            force => Force
        },
        #{channel => Channel}
    ),
    case Ret of
        {ok, _, _} -> ok;
        R -> R
    end.

lookup_stream(Channel, StreamName, OrderingKey) ->
    Ret = hstream_server_h_stream_api_client:lookup_stream(
        #{
            streamName => StreamName,
            orderingKey => OrderingKey
        },
        #{channel => Channel}
    ),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(serverNode, Resp)};
        R ->
            R
    end.

% --------------------------------------------------------------------------------

list_subscriptions(Channel) ->
    Ret = hstream_server_h_stream_api_client:list_subscriptions(
        #{},
        #{channel => Channel}
    ),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(subscription, Resp)};
        R ->
            R
    end.

create_subscription(Channel, SubscriptionId, StreamName) ->
    Ret = hstream_server_h_stream_api_client:create_subscription(
        #{
            subscriptionId => SubscriptionId,
            streamName => StreamName
        },
        #{
            channel => Channel
        }
    ),
    case Ret of
        {ok, _, _} ->
            ok;
        R ->
            R
    end.
