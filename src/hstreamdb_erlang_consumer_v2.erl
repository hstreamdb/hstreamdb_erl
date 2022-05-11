-module(hstreamdb_erlang_consumer_v2).

-export([start/4, ack/2, get_record_id/1, get_record/1]).

% --------------------------------------------------------------------------------

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

ack(Stream, AckId) when is_map(AckId) ->
    ack(Stream, [AckId]);
ack(Stream, AckIds) when is_list(AckIds) ->
    {StreamingFetchStream, StreamingFetchRequestBuilder} = Stream,
    grpc_client:send(
        StreamingFetchStream,
        StreamingFetchRequestBuilder(AckIds)
    ).

get_record_id(ReceivedRecord) ->
    maps:get(recordId, ReceivedRecord).

get_record(ReceivedRecord) ->
    maps:get(record, ReceivedRecord).
