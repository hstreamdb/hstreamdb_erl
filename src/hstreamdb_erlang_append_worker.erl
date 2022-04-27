-module(hstreamdb_erlang_append_worker).

-behaviour(gen_server).
-export([init/1, handle_call/3]).

-export([build_start_args/1, build_appender_option/2]).

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

build_appender_option(ServerUrl, StreamName) ->
    #{
        server_url => ServerUrl,
        stream_name => StreamName
    }.

build_appender_resource(Channels) when is_map(Channels) ->
    #{
        channels => Channels
    }.

% --------------------------------------------------------------------------------

get_channel(ServerUrl, AppenderState) ->
    #{appender_option := AppenderOption, appender_resource := #{channels := Channels}} =
        AppenderState,
    case maps:is_key(ServerUrl, AppenderState) of
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
