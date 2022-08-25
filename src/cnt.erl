-module(cnt).

-behaviour(gen_server).

-export([init/1, handle_info/2, handle_call/3, handle_cast/2]).

init([X]) ->
  io:format("start time: ~p~n", [erlang:system_time()]),
  Timer = erlang:send_after(1, self(), check),
  {ok, {Timer, 0, X}}.

handle_info(check, {OldTimer, Cnt, X}) ->
  erlang:cancel_timer(OldTimer),
  GetVal = atomics:get(X, 1),
  io:format("~p ~p ~p~n", [GetVal, Cnt, GetVal / 1000 / 1000 / 10]),
  atomics:put(X, 1, 0),
  Timer = erlang:send_after(10000, self(), check),
  {noreply, {Timer, Cnt + 1, X}}.

handle_call(_R, _F, S) ->
  {reply, ok, S}.

handle_cast(R, S) ->
  handle_info(R, S).
