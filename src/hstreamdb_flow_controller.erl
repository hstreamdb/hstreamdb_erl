-module(hstreamdb_flow_controller).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([ok_msg/0]).

-record(state,
        {flow_control_timer :: timer:tref(),
         flow_control_size,
         current_size,
         flow_control_memory,
         current_memory_size}).

ok_msg() ->
  {flow_control, ok}.

init(Options) ->
  Interval = proplists:get_value(flow_control_interval, Options),
  Size = proplists:get_value(flow_control_size, Options),
  MemSize = proplists:get_value(flow_control_memory, Options),
  CurMemSize = proplists:get_value(total, erlang:memory()),
  true = CurMemSize /= undefined,
  {ok,
   #state{flow_control_timer = timer:send_interval(Interval, refresh),
          flow_control_size = Size,
          flow_control_memory = MemSize,
          current_size = CurMemSize}}.

handle_call({check, MsgSize},
            {From, _},
            State =
              #state{flow_control_size = Size,
                     current_size = CurSize,
                     flow_control_memory = MemSize,
                     current_memory_size = CurMemSize}) ->
  do_check_and_send(Size, CurSize, MemSize, CurMemSize, From),
  {reply, ok, State#state{current_size = CurSize + MsgSize}};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(Request, State) ->
  handle_info(Request, State).

handle_info(refresh, State) ->
  {noreply, State#state{current_size = 0}};
handle_info({check, From},
            State =
              #state{flow_control_size = Size,
                     current_size = CurSize,
                     flow_control_memory = MemSize,
                     current_memory_size = CurMemSize}) ->
  do_check_and_send(Size, CurSize, MemSize, CurMemSize, From),
  {noreply, State};
handle_info(refresh_mem, State) ->
  CurMemSize = proplists:get_value(total, erlang:memory()),
  true = CurMemSize /= undefined,
  {noreply, State#state{current_memory_size = CurMemSize}};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{flow_control_timer = Timer}) ->
  _ = timer:cancel(Timer),
  ok.

do_check_and_send(_Size, _CurSize, MemSize, CurMemSize, From) ->
  % if Size >= CurSize ->
  if MemSize > CurMemSize ->
       From ! ok_msg();
     true ->
       io:format("[DEBUG]: memory check failed, memory limit = ~p, total memory "
                 "= ~p, binary = ~p~n",
                 [MemSize, CurMemSize, proplists:get_value(binary, erlang:memory())]),
       timer:send_after(500, {check, From})
  end.
