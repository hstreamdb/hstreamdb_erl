hstreamdb_erlang
=====

An OTP library for HStreamDB

Build
-----

    $ rebar3 compile


Example usage:

```erl
readme() ->
    hstreamdb_erlang:start(normal, []),

    hstreamdb_erlang:start_client_channel(ch, "http://127.0.0.1:6570"),
    io:format("~p~n", [hstreamdb_erlang:list_streams(ch)]),

    io:format("~p~n", [hstreamdb_erlang:delete_stream(ch, "test_stream")]),
    io:format("~p~n", [hstreamdb_erlang:list_streams(ch)]),

    io:format("~p~n", [hstreamdb_erlang:create_stream(ch, "test_stream", 3, 14)]),
    io:format("~p~n", [hstreamdb_erlang:list_streams(ch)]),

    XS = lists:seq(0, 100),
    lists:foreach(
        fun(X) ->
            io:format("~p: ~p~n", [
                X,
                hstreamdb_erlang:append(
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
```
