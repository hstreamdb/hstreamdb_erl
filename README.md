hstreamdb_erlang
=====

An OTP library for HStreamDB

Build
-----

    $ rebar3 compile


Example usage (without lib module prefix):

```erl
readme() ->
    StreamName = string_format("test_stream-~p", [erlang:system_time(second)]),
    ChannelName = string_format("test_channel-~p", [erlang:system_time(second)]),

    start(normal, []),

    io:format("~p~n", [start_client_channel(ChannelName, "http://127.0.0.1:6570")]),
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
```

which can be load from `rebar3 shell` `hstreamdb_erlang:readme()`.
