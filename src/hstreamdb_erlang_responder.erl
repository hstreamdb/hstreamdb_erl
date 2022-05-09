-module(hstreamdb_erlang_responder).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2]).

% --------------------------------------------------------------------------------

init(_Args) -> undefined.

% --------------------------------------------------------------------------------

build_responder_state(ResponderStatus, ResponderResource) ->
    #{
        responder_status => ResponderStatus,
        responder_resource => ResponderResource
    }.

build_responder_status(AckIds) ->
    #{
        ack_ids => AckIds
    }.

build_responder_resource({Stream, Builder}) ->
    #{
        stream => {Stream, Builder}
    }.

neutral_responder_status() ->
    AckIds = [],
    build_responder_status(AckIds).

% --------------------------------------------------------------------------------

get_ack_ids(ResponderState) ->
    #{responder_status := #{ack_ids := AckIds}} = ResponderState,
    AckIds.

add_to_buffer(
    RecordId,
    #{
        responder_status := ResponderStatus,
        responder_resource := ResponderResource
    } = _State
) ->
    #{
        ack_ids := AckIds
    } = ResponderStatus,
    NewAckIds = [RecordId | AckIds],
    NewResponderStatus = build_responder_status(NewAckIds),
    build_responder_state(NewResponderStatus, ResponderResource).

clear_buffer(State) ->
    #{responder_resource := ResponderResource} = State,
    NewAckIds = [],
    NewResponderStatus = build_responder_status(NewAckIds),
    build_responder_state(NewResponderStatus, ResponderResource).

% --------------------------------------------------------------------------------

handle_call({Method, Body} = _Request, _From, State) ->
    case Method of
        ack -> exec_ack(Body, State)
    end.

handle_cast(_, _) -> undefined.

% --------------------------------------------------------------------------------

build_ack_request(RecordId) ->
    #{
        record_id => RecordId
    }.

exec_ack(
    #{
        record_id := RecordId
    } = AckRequest,
    #{
        responder_resource := ResponderResource
    } = State
) ->
    State0 = add_to_buffer(RecordId, State),
    #{stream := {Stream, Builder}} = ResponderResource,
    #{responder_status := #{ack_ids := AckIds}} = State0,
    Reply = do_flush(AckIds, {Stream, Builder}),
    NewState = clear_buffer(State0),
    {reply, Reply, NewState}.

do_flush(AckIds, {Stream, Builder}) ->
    grpc_client:send(Stream, Builder(AckIds)).

% --------------------------------------------------------------------------------

new_responder_stream(Channel, SubscriptionId, ConsumerName, OrderingKey) ->
    AckIds = [],
    StreamingFetchRequestBuilder = fun(X) ->
        #{
            subscriptionId => SubscriptionId,
            consumerName => ConsumerName,
            orderingKey => OrderingKey,
            ackIds => X
        }
    end,
    StreamingFetchRequest = StreamingFetchRequestBuilder(AckIds),
    {
        hstream_server_h_stream_api_client:streaming_fetch(StreamingFetchRequest, #{
            channel => Channel
        }),
        StreamingFetchRequestBuilder
    }.
