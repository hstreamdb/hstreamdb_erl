-module(hstreamdb_erlang).

-behaviour(supervisor).
-behaviour(application).

-export([start/2, stop/1]).
-export([init/1]).

-export([start_client_channel/2, start_client_channel/3, stop_client_channel/1]).
-export([server_node_to_host_port/1]).

-export([list_streams/1, create_stream/4, lookup_stream/3]).
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

start_client_channel(ChName, URL) ->
    hstreamdb_erlang_client:create_channel_pool(ChName, URL).

start_client_channel(ChName, URL, Opts) ->
    hstreamdb_erlang_client:create_channel_pool(ChName, URL, Opts).

stop_client_channel(ChName) -> hstreamdb_erlang_client:stop_channel_pool(ChName).

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

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).

server_node_to_host_port(ServerNode) ->
    Host = maps:get(host, ServerNode),
    Port = maps:get(port, ServerNode),
    string_format("~s:~p", [Host, Port]).

server_node_to_host_port(ServerNode, Protocol) ->
    Host = maps:get(host, ServerNode),
    Port = maps:get(port, ServerNode),
    string_format("~p://~s:~p", [Protocol, Host, Port]).

%%--------------------------------------------------------------------

append(Ch, StreamName, OrderingKey, PayloadType, Payload) ->
    {ok, ServerNode} = lookup_stream(Ch, StreamName, OrderingKey),
    ServerUrl = server_node_to_host_port(ServerNode, http),
    logger:notice(
        "hstreamdb_erlang:append: lookup_stream_server_url = ~s",
        [ServerUrl]
    ),
    Flag =
        case PayloadType of
            json -> 0;
            raw -> 1
        end,
    Timestamp = #{
        seconds => erlang:system_time(second),
        nanos => erlang:system_time(nanosecond)
    },
    Header = #{
        flag => Flag,
        publish_time => Timestamp,
        key => OrderingKey
    },
    Record = #{
        header => Header,
        payload => Payload
    },
    io:format("~p~n", [start_client_channel(innternal_append_ch, ServerUrl)]),

    Req = #{
        streamName => StreamName,
        records => [Record]
    },
    logger:notice(
        "hstreamdb_erlang:append: request = ~p",
        [Req]
    ),
    Ret = hstream_server_h_stream_api_client:append(
        Req,
        #{channel => innternal_append_ch}
    ),

    % stop_client_channel(innternal_append_ch),
    case Ret of
        {ok, Resp, _} ->
            {ok, maps:get(recordIds, Resp)};
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
    start(normal, []),

    start_client_channel(ch, "http://127.0.0.1:6570"),
    io:format("~p~n", [list_streams(ch)]),

    % io:format("~p~n", [delete_stream(ch, "test_stream")]),
    io:format("~p~n", [list_streams(ch)]),

    io:format("~p~n", [create_stream(ch, "test_stream", 3, 14)]),
    io:format("~p~n", [list_streams(ch)]),

    io:format("~p~n", [lookup_stream(ch, "test_stream", "")]),

    XS = lists:seq(0, 100),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                append(
                    ch,
                    "test_stream",
                    "",
                    raw,
                    <<"this_is_a_binary_literal">>
                )
            ])
        end,
        XS
    ),
    ok.
