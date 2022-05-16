-module(hstreamdb_erlang).

-export([
    start_client_channel/1, start_client_channel/2,
    stop_client_channel/1,
    with_client_channel/2, with_client_channel/3
]).

-export([list_streams/1, create_stream/4, delete_stream/2, delete_stream/3]).

-export([
    list_subscriptions/1,
    create_subscription/3, create_subscription/5,
    delete_subscription/2, delete_subscription/3
]).

-export_type([delete_stream_opts/0, delete_subscription_opts/0]).

% --------------------------------------------------------------------------------

-type hstreamdb_channel() :: string().

-spec start_client_channel(ServerUrl :: string()) -> {ok, hstreamdb_channel()}.

%% @doc Start HStreamDB client channel.

start_client_channel(ServerUrl) ->
    start_client_channel(ServerUrl, #{}).

-spec start_client_channel(ServerUrl :: string(), Opts :: grpc_client_sup:options()) ->
    {ok, hstreamdb_channel()}.

%% @doc Start HStreamDB client channel with gRPC options.

start_client_channel(ServerUrl, Opts) ->
    ChannelName = "hstream_client_channel-" ++ hstreamdb_erlang_utils:uid(),
    case grpc_client_sup:create_channel_pool(ChannelName, ServerUrl, Opts) of
        {error, _} = E -> E;
        _ -> {ok, ChannelName}
    end.

-spec stop_client_channel(Channel :: string()) -> ok.

%% @doc Stop HStreamDB client channel.

stop_client_channel(Channel) -> grpc_client_sup:stop_channel_pool(Channel).

-spec with_client_channel(ServerUrl :: string(), Fun :: fun()) -> any().

with_client_channel(ServerUrl, Fun) ->
    with_client_channel(ServerUrl, #{}, Fun).

-spec with_client_channel(ServerUrl :: string(), Opts :: grpc_client_sup:options(), Fun :: fun()) ->
    any().

with_client_channel(ServerUrl, Opts, Fun) ->
    {ok, Ch} = start_client_channel(ServerUrl, Opts),
    Ret = {Ch, Fun(Ch)},
    _ = stop_client_channel(Ch),
    Ret.

% --------------------------------------------------------------------------------

-spec list_streams(Channel :: hstreamdb_channel()) ->
    {ok, Streams :: list('HStreamApi':stream())}
    | {error, Reason :: term()}.

%% @doc List all streams.

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

-spec create_stream(
    Channel :: hstreamdb_channel(),
    StreamName :: string(),
    ReplicationFactor :: integer(),
    BacklogDuration :: integer()
) -> ok | {error, Reason :: term()}.

%% @doc Create a new stream.

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

-spec delete_stream(Channel :: hstreamdb_channel(), StreamName :: string()) ->
    ok | {error, Reason :: term()}.

%% @doc Delete the specified stream with the name of stream.

delete_stream(Channel, StreamName) -> delete_stream(Channel, StreamName, #{}).

-spec delete_stream(
    Channel :: hstreamdb_channel(), StreamName :: string(), Opts :: delete_stream_opts()
) ->
    ok | {error, Reason :: term()}.

-type delete_stream_opts() :: map().

%% @doc
%% Delete the specified stream with the name of stream.
%% Avaiable options are: `#{ignoreNonExist => bool, force => bool}'
%% @end

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

% --------------------------------------------------------------------------------

%% @doc List all subscriptions.

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

%% @doc Create a new subscription with 600 seconds ack timeout, 10000 max unacked records.

create_subscription(Channel, SubscriptionId, StreamName) ->
    AckTimeoutSeconds = 600,
    MaxUnackedRecords = 10000,
    create_subscription(Channel, SubscriptionId, StreamName, AckTimeoutSeconds, MaxUnackedRecords).

-spec create_subscription(
    Channel :: hstreamdb_channel(),
    SubscriptionId :: string(),
    StreamName :: string(),
    AckTimeoutSeconds :: integer(),
    MaxUnackedRecords :: integer()
) -> ok | {error, Reason :: term()}.

%% @doc Create a new Subscription.
create_subscription(Channel, SubscriptionId, StreamName, AckTimeoutSeconds, MaxUnackedRecords) ->
    true = AckTimeoutSeconds > 0 andalso AckTimeoutSeconds < 36000,
    true = MaxUnackedRecords > 0,

    Ret = hstream_server_h_stream_api_client:create_subscription(
        #{
            subscriptionId => SubscriptionId,
            streamName => StreamName,
            ackTimeoutSeconds => AckTimeoutSeconds,
            maxUnackedRecords => MaxUnackedRecords
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

-type delete_subscription_opts() :: map().

-spec delete_subscription(
    Channel :: hstreamdb_channel(), SubscriptionId :: string()
) -> ok | {error, Reason :: term()}.

%% @doc Delete (force) the specified subscription with the name of subscription.

delete_subscription(
    Channel, SubscriptionId
) ->
    delete_subscription(Channel, SubscriptionId, #{}).

-spec delete_subscription(
    Channel :: hstreamdb_channel(), SubscriptionId :: string(), Opts :: delete_subscription_opts()
) -> ok | {error, Reason :: term()}.

%% @doc
%% Delete the specified subscription with the name of subscription.
%% Avaiable options are: `#{force => bool}'
%% @end

delete_subscription(
    Channel, SubscriptionId, Opts
) ->
    Force = maps:get(force, Opts, true),
    Ret = hstream_server_h_stream_api_client:delete_subscription(
        #{
            subscriptionId => SubscriptionId,
            force => Force
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
