-module(hstreamdb_erlang_bench).

%% API exports
-export([main/1]).

%%====================================================================
%% API functions
%%====================================================================

%% escript Entry point
main(Args) ->
    io:format("Args: ~p~n", [Args]),

    _ =
        case Args of
            ["readme"] ->
                readme();
            _ ->
                ok
        end,

    erlang:halt(0).

%%====================================================================
%% Internal functions
%%====================================================================

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).

readme() ->
    StreamName = string_format("test_stream-~p", [erlang:system_time(second)]),

    hstreamdb_erlang:start(normal, []),

    StartClientChannelRet = hstreamdb_erlang:start_client_channel("http://127.0.0.1:6570"),
    io:format("~p~n", [StartClientChannelRet]),
    {ok, ChannelName} = StartClientChannelRet,

    io:format("~p~n", [hstreamdb_erlang:list_streams(ChannelName)]),

    io:format("~p~n", [hstreamdb_erlang:create_stream(ChannelName, StreamName, 3, 14)]),
    io:format("~p~n", [hstreamdb_erlang:list_streams(ChannelName)]),

    XS = lists:seq(0, 100),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                hstreamdb_erlang:append(
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
                hstreamdb_erlang:append(
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

    io:format("~p~n", [hstreamdb_erlang:delete_stream(ChannelName, StreamName)]),
    io:format("~p~n", [hstreamdb_erlang:list_streams(ChannelName)]),

    io:format("~p~n", [hstreamdb_erlang:stop_client_channel(ChannelName)]),

    ok.

bit_size_128() -> (<<"___hstream.io___">>).

get_bytes(Size, Unit) ->
    SizeBytes =
        case Unit of
            k -> Size * 1024;
            m -> Size * 1024 * 1024
        end,
    lists:replicate(SizeBytes / 128, bit_size_128()).
