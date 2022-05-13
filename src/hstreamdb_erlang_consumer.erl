-module(hstreamdb_erlang_consumer).

-export([start/4, ack/2, get_record_id/1, get_record/1]).

% --------------------------------------------------------------------------------

-type record_id() :: map().

-type responder() :: {grpc_client:grpcstream(), fun()}.

-type received_record() :: #{
    recordId => record_id(),
    record => binary()
}.

-type consumer_fun() :: fun((responder(), received_record()) -> any()).

-spec start(
    ServerUrl :: string(),
    SubscriptionId :: string(),
    ConsumerName :: string(),
    ConsumerFun :: consumer_fun()
) -> any().

start(ServerUrl, SubscriptionId, ConsumerName, ConsumerFun) ->
    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    {ok,
        #{
            serverNode := ServerNode
        },
        _} = hstream_server_h_stream_api_client:lookup_subscription(
        #{
            subscriptionId => SubscriptionId
        },
        #{
            channel => Channel
        }
    ),
    ok = hstreamdb_erlang:stop_client_channel(Channel),
    SubscriptionServerUrl = hstreamdb_erlang:server_node_to_host_port(ServerNode, http),
    {ok, SubscriptionChannel} = hstreamdb_erlang:start_client_channel(SubscriptionServerUrl),

    StreamingFetchRequestBuilder = fun(AckIds) ->
        #{
            subscriptionId => SubscriptionId,
            consumerName => ConsumerName,
            ackIds => AckIds
        }
    end,

    AckIds = [],
    InitStreamingFetchRequest = StreamingFetchRequestBuilder(AckIds),
    {ok, StreamingFetchStream} = hstream_server_h_stream_api_client:streaming_fetch(
        #{},
        #{
            channel => SubscriptionChannel
        }
    ),
    LoopRecv = fun LoopRecvFun() ->
        Recv = grpc_client:recv(StreamingFetchStream),
        case Recv of
            {ok, RecvXS} when not is_tuple(RecvXS) ->
                lists:foreach(
                    fun(RecvX) ->
                        #{receivedRecords := ReceivedRecords} = RecvX,
                        lists:foreach(
                            fun(ReceivedRecord) ->
                                ConsumerFun(
                                    {StreamingFetchStream, StreamingFetchRequestBuilder},
                                    ReceivedRecord
                                )
                            end,
                            ReceivedRecords
                        )
                    end,
                    RecvXS
                ),
                LoopRecvFun()
        end
    end,
    timer:sleep(200),
    ok = grpc_client:send(StreamingFetchStream, InitStreamingFetchRequest),
    LoopRecv().

-spec ack(Responder, AckIds) -> ok when
    Responder :: responder(),
    AckIds :: record_id() | list(record_id()).

ack(Responder, AckId) when is_map(AckId) ->
    ack(Responder, [AckId]);
ack(Responder, AckIds) when is_list(AckIds) ->
    {StreamingFetchStream, StreamingFetchRequestBuilder} = Responder,
    grpc_client:send(
        StreamingFetchStream,
        StreamingFetchRequestBuilder(AckIds)
    ).

-spec get_record_id(ReceivedRecord :: received_record()) -> record_id().

get_record_id(ReceivedRecord) ->
    maps:get(recordId, ReceivedRecord).

-spec get_record(ReceivedRecord :: received_record()) -> binary().

get_record(ReceivedRecord) ->
    maps:get(record, ReceivedRecord).
