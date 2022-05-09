-module(hstreamdb_erlang_consumer).

-behaviour(gen_server).

% --------------------------------------------------------------------------------

build_start_args(ServerUrl, ReturnPid, SubscriptionId, ConsumerName, OrderingKey) ->
    #{
        server_url => ServerUrl,
        return_pid => ReturnPid,
        subscriptionId => SubscriptionId,
        consumerName => ConsumerName,
        orderingKey => OrderingKey
    }.

init(Args) ->
    #{
        server_url := ServerUrl,
        return_pid := ReturnPid,
        subscriptionId := SubscriptionId,
        consumerName := ConsumerName,
        orderingKey := OrderingKey
    } = Args,

    ConsumerOption = build_consumer_option(ReturnPid),

    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    Stream = new_consumer_stream(Channel, SubscriptionId, ConsumerName, OrderingKey),
    ConsumerResource = build_consumer_resource(Stream),

    State = build_consumer_state(ConsumerOption, ConsumerResource),
    {ok, State}.

% --------------------------------------------------------------------------------

build_consumer_option(ReturnPid) ->
    #{
        return_pid => ReturnPid
    }.

build_consumer_resource(Stream) ->
    #{
        stream => Stream
    }.

build_consumer_state(ConsumerOption, ConsumerResource) ->
    #{
        consumer_option => ConsumerOption,
        consumer_resource => ConsumerResource
    }.

% --------------------------------------------------------------------------------

new_consumer_stream(Channel, SubscriptionId, ConsumerName, OrderingKey) ->
    AckIds = [],
    StreamingFetchRequest =
        #{
            subscriptionId => SubscriptionId,
            consumerName => ConsumerName,
            orderingKey => OrderingKey,
            ackIds => AckIds
        },
    {ok, StreamingFetchStream} = hstream_server_h_stream_api_client:streaming_fetch(
        StreamingFetchRequest, #{
            channel => Channel
        }
    ),
    StreamingFetchStream.
