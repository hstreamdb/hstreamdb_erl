-module(hstreamdb_erlang_utils).

-export([string_format/2]).
-export([countdown/2]).

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).

countdown(N, Pid) ->
    spawn(
        fun() -> do_countdown(N, Pid) end
    ).

do_countdown(N, Pid) ->
    case N of
        0 ->
            Pid ! finished;
        _ ->
            receive
                finished ->
                    do_countdown(N - 1, Pid)
            end
    end.
