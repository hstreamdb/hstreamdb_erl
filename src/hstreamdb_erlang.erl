-module(hstreamdb_erlang).

-behaviour(supervisor).
-behaviour(application).

-export([start/2, stop/1]).
-export([init/1]).

-export([start_client_channel/2, start_client_channel/3]).
-export([server_node_to_host_port/1]).

-export([list_streams/1, create_stream/4, lookup_stream/3]).

-export([list_subscriptions/1]).

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

%%--------------------------------------------------------------------

list_streams(Ch) ->
    Ret = hstream_server_h_stream_api_client:list_streams(#{}, #{
        channel => Ch
    }),
    case Ret of
        {ok, Resp, _} -> {ok, maps:get(streams, Resp)};
        R -> R
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
        {ok, Resp, _} -> {ok, maps:get(serverNode, Resp)};
        R -> R
    end.

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).

server_node_to_host_port(ServerNode) ->
    Host = maps:get(host, ServerNode),
    Port = maps:get(port, ServerNode),
    string_format("~s:~p", [Host, Port]).

%%--------------------------------------------------------------------

list_subscriptions(Ch) ->
    Ret = hstream_server_h_stream_api_client:list_subscriptions(#{}, #{
        channel => Ch
    }),
    case Ret of
        {ok, Resp, _} -> {ok, maps:get(subscription, Resp)};
        R -> R
    end.
