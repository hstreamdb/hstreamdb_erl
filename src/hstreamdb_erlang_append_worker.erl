-module(hstreamdb_erlang_append_worker).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2]).

-export([build_start_args/1, build_appender_option/3, build_append_request/2]).

% --------------------------------------------------------------------------------

build_start_args(AppenderOption) ->
    #{
        appender_option => AppenderOption
    }.

init(
    #{
        appender_option := AppenderOption
    } = _Args
) ->
    #{
        server_url := ServerUrl
    } = AppenderOption,
    {_, AppenderState} = get_channel(
        ServerUrl, build_appender_state(AppenderOption, build_appender_resource(#{}))
    ),
    {ok, AppenderState}.

% --------------------------------------------------------------------------------

build_appender_state(AppenderOption, AppenderResource) ->
    #{
        appender_option => AppenderOption,
        appender_resource => AppenderResource
    }.

build_appender_option(ServerUrl, StreamName, ReturnPid) ->
    #{
        server_url => ServerUrl,
        stream_name => StreamName,
        return_pid => ReturnPid
    }.

build_appender_resource(Channels) when is_map(Channels) ->
    #{
        channels => Channels
    }.

% --------------------------------------------------------------------------------

get_channel(ServerUrl, AppenderState) ->
    #{appender_option := AppenderOption, appender_resource := #{channels := Channels}} =
        AppenderState,
    case maps:is_key(ServerUrl, Channels) of
        true ->
            {maps:get(ServerUrl, Channels), AppenderState};
        false ->
            {ok, Channel} = hstreamdb_erlang:start_client_channel(ServerUrl),
            NewAppenderResource = build_appender_resource(maps:put(ServerUrl, Channel, Channels)),
            {Channel, build_appender_state(AppenderOption, NewAppenderResource)}
    end.

% --------------------------------------------------------------------------------

handle_call(_, _, _) ->
    throw(hstreamdb_exception).

handle_cast({Method, Body} = _Request, State) ->
    case Method of
        append -> exec_append(Body, State)
    end.

% --------------------------------------------------------------------------------

build_record_header(PayloadType, OrderingKey) ->
    Flag =
        case PayloadType of
            json -> 0;
            raw -> 1
        end,

    Timestamp = #{
        seconds => erlang:system_time(second),
        nanos => erlang:system_time(nanosecond)
    },

    #{
        flag => Flag,
        publish_time => Timestamp,
        key => OrderingKey
    }.

build_append_request(OrderingKey, Payloads) ->
    {append, {OrderingKey, Payloads}}.

exec_append(
    {OrderingKey, Payloads},
    #{
        appender_option := AppenderOption
    } = State
) ->
    #{
        server_url := ServerUrl,
        stream_name := StreamName,
        return_pid := ReturnPid
    } = AppenderOption,

    AppendRecords = lists:map(
        fun({PayloadType, Payload}) ->
            RecordHeader = build_record_header(PayloadType, OrderingKey),
            #{
                header => RecordHeader,
                payload => Payload
            }
        end,
        Payloads
    ),

    {Channel, State0} = get_channel(ServerUrl, State),
    {ok, ServerNode} = lookup_stream(Channel, StreamName, OrderingKey),

    AppendServerUrl = hstreamdb_erlang_utils:server_node_to_host_port(ServerNode, http),
    {InternalChannel, State1} = get_channel(AppendServerUrl, State0),

    {ok, #{recordIds := RecordIds}, _} =
        case
            hstream_server_h_stream_api_client:append(
                #{
                    streamName => StreamName,
                    records => AppendRecords
                },
                #{channel => InternalChannel}
            )
        of
            {ok, _, _} = AppendRet ->
                AppendRet;
            E ->
                logger:error("append error: ~p~n", [E]),
                hstreamdb_erlang_utils:throw_hstreamdb_exception(E)
        end,
    ReturnPid ! {record_ids, RecordIds},

    NewState = State1,
    {noreply, NewState}.

% --------------------------------------------------------------------------------

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
