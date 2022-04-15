-module(hstreamdb_erlang_utils).

-export([string_format/2]).

string_format(Pattern, Values) ->
    lists:flatten(io_lib:format(Pattern, Values)).
