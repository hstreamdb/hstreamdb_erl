-module(hstreamdb_erlang_responder).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2]).

% --------------------------------------------------------------------------------

init(_Args) -> undefined.

% --------------------------------------------------------------------------------

build_responder_state(ResponderStatus) ->
    #{
        responder_status => ResponderStatus
    }.

build_responder_status(AckIds) ->
    #{
        ack_ids => AckIds
    }.

neutral_responder_status() ->
    AckIds = [],
    build_responder_status(AckIds).

% --------------------------------------------------------------------------------

add_to_buffer(
    RecordId,
    #{responder_status := ResponderStatus} = _State
) ->
    #{
        ack_ids := AckIds
    } = ResponderStatus,
    NewAckIds = [RecordId | AckIds],
    NewResponderStatus = build_responder_status(NewAckIds),
    build_responder_state(NewResponderStatus).

clear_buffer(_State) ->
    NewAckIds = [],
    NewResponderStatus = build_responder_status(NewAckIds),
    build_responder_state(NewResponderStatus).

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
    State
) ->
    Reply = #{},
    NewState = State,
    {reply, Reply, NewState}.

% --------------------------------------------------------------------------------

do_flush(AckIds, ServerUrl, SubscriptionId, ConsumerName, OrderingKey) ->
    {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
    _ = hstreamdb_erlang:stop_client_channel(Channel).
