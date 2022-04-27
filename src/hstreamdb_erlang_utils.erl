-module(hstreamdb_erlang_utils).

-export([string_format/2]).
-export([countdown/2]).
-export([throw_hstreamdb_exception/1]).
-export([uid/0]).

string_format(Pattern, Values) ->
    lists:flatten(
        io_lib:format(Pattern, Values)
    ).

countdown(N, Pid) ->
    spawn(fun() -> do_countdown(N, Pid) end).

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

throw_hstreamdb_exception(E) ->
    throw({hstreamdb_exception, E}).

uid() ->
    {X0, X1, X2} = erlang:timestamp(),
    hstreamdb_erlang_utils:string_format(
        "~p-~p-~p_~p_~p-~p",
        [node(), self(), X0, X1, X2, erlang:unique_integer()]
    ).
