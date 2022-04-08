-module(hstreamdb_erlang).

-behaviour(supervisor).
-behaviour(application).

-export([start/2, stop/1]).
-export([init/1]).

-export([start_client_channel/1, start_client_channel/2, stop_client_channel/1]).

-export([list_streams/1, create_stream/4, delete_stream/2, delete_stream/3]).
-export([append/5]).

-export([list_subscriptions/1]).

-export([readme/0]).

%%--------------------------------------------------------------------
%% APIs for application

start(_StartType, _StartArgs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop(_State) ->
    ok.

%%--------------------------------------------------------------------
%% callbacks for supervisor

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

%%--------------------------------------------------------------------

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).

uid() ->
    {X0, X1, X2} = erlang:timestamp(),
    string_format("~p-~p_~p_~p-~p", [
        node(),
        X0,
        X1,
        X2,
        erlang:unique_integer()
    ]).

server_node_to_host_port(ServerNode) ->
    Host = maps:get(host, ServerNode),
    Port = maps:get(port, ServerNode),
    string_format("~s:~p", [Host, Port]).

server_node_to_host_port(ServerNode, Protocol) ->
    string_format("~p://~s", [Protocol, server_node_to_host_port(ServerNode)]).

%%--------------------------------------------------------------------

start_client_channel(URL) ->
    start_client_channel(URL, #{}).

start_client_channel(URL, Opts) ->
    ChName = "hstream_client_channel-" ++ uid(),
    case hstreamdb_erlang_client:create_channel_pool(ChName, URL, Opts) of
        {error, _} = E -> E;
        _ -> {ok, ChName}
    end.

stop_client_channel(Channel) -> hstreamdb_erlang_client:stop_channel_pool(Channel).

%%--------------------------------------------------------------------

list_streams(Ch) ->
    Ret = hstream_server_h_stream_api_client:list_streams(
        #{},
        #{channel => Ch}
    ),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(streams, Resp)};
        R ->
            R
    end.

create_stream(Ch, StreamName, ReplicationFactor, BacklogDuration) ->
    Ret = hstream_server_h_stream_api_client:create_stream(
        #{
            streamName => StreamName,
            replicationFactor => ReplicationFactor,
            backlogDuration => BacklogDuration
        },
        #{channel => Ch}
    ),
    case Ret of
        {ok, _, _} -> {ok};
        R -> R
    end.

delete_stream(Ch, StreamName) -> delete_stream(Ch, StreamName, #{}).

delete_stream(Ch, StreamName, Opts) ->
    IgnoreNonExist = maps:get(ignoreNonExist, Opts, false),
    Force = maps:get(force, Opts, false),

    Ret = hstream_server_h_stream_api_client:delete_stream(
        #{
            streamName => StreamName,
            ignoreNonExist => IgnoreNonExist,
            force => Force
        },
        #{channel => Ch}
    ),
    case Ret of
        {ok, _, _} -> {ok};
        R -> R
    end.

lookup_stream(Ch, StreamName, OrderingKey) ->
    Ret = hstream_server_h_stream_api_client:lookup_stream(
        #{
            streamName => StreamName,
            orderingKey => OrderingKey
        },
        #{channel => Ch}
    ),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(serverNode, Resp)};
        R ->
            R
    end.

%%--------------------------------------------------------------------

make_record_header(PayloadType, OrderingKey) ->
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

make_record(Header, Payload) ->
    #{
        header => Header,
        payload => Payload
    }.

make_records(Header, Payload) when is_list(Payload) ->
    lists:map(
        fun(X) -> make_record(Header, X) end, Payload
    );
make_records(Header, Payload) when is_binary(Payload) ->
    [
        make_record(Header, Payload)
    ].

append(Ch, StreamName, OrderingKey, PayloadType, Payload) ->
    case lookup_stream(Ch, StreamName, OrderingKey) of
        {ok, ServerNode} ->
            ServerUrl = server_node_to_host_port(ServerNode, http),
            % logger:notice(
            %     "hstreamdb_erlang:append: lookup_stream_server_url = ~s",
            %     [ServerUrl]
            % ),

            Header = make_record_header(PayloadType, OrderingKey),
            Records = make_records(Header, Payload),

            {ok, InternalChannelName} = start_client_channel(ServerUrl),
            Ret = hstream_server_h_stream_api_client:append(
                #{
                    streamName => StreamName,
                    records => Records
                },
                #{channel => InternalChannelName}
            ),
            stop_client_channel(InternalChannelName),

            case Ret of
                {ok, Resp, _} ->
                    {ok, maps:get(recordIds, Resp)};
                R ->
                    R
            end;
        R ->
            R
    end.

%%--------------------------------------------------------------------

list_subscriptions(Ch) ->
    Ret = hstream_server_h_stream_api_client:list_subscriptions(
        #{},
        #{channel => Ch}
    ),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(subscription, Resp)};
        R ->
            R
    end.

%%--------------------------------------------------------------------

readme() ->
    StreamName = string_format("test_stream-~p", [erlang:system_time(second)]),

    start(normal, []),

    StartClientChannelRet = start_client_channel("http://127.0.0.1:6570"),
    io:format("~p~n", [StartClientChannelRet]),
    {ok, ChannelName} = StartClientChannelRet,

    io:format("~p~n", [list_streams(ChannelName)]),

    io:format("~p~n", [create_stream(ChannelName, StreamName, 3, 14)]),
    io:format("~p~n", [list_streams(ChannelName)]),

    io:format("~p~n", [lookup_stream(ChannelName, StreamName, "")]),

    XS = lists:seq(0, 100),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                append(
                    ChannelName,
                    StreamName,
                    "",
                    raw,
                    <<"this_is_a_binary_literal">>
                )
            ])
        end,
        XS
    ),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                append(
                    ChannelName,
                    StreamName,
                    "",
                    raw,
                    lists:duplicate(10, <<"this_is_a_binary_literal">>)
                )
            ])
        end,
        XS
    ),

    io:format("~p~n", [delete_stream(ChannelName, StreamName)]),
    io:format("~p~n", [list_streams(ChannelName)]),

    io:format("~p~n", [stop_client_channel(ChannelName)]),

    ok.
