-module(hstreamdb_flow_controller).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([ok_msg/0]).

-record(state, {flow_control_timer :: timer:tref(), flow_control_size, current_size}).

ok_msg() ->
  {flow_control, ok}.

init(Options) ->
  Interval = proplists:get_value(flow_control_interval, Options),
  Size = proplists:get_value(flow_control_size, Options),
  {ok,
   #state{flow_control_timer = timer:send_interval(Interval, refresh),
          flow_control_size = Size,
          current_size = 0}}.

handle_call({check, MsgSize},
            {From, _},
            State = #state{flow_control_size = Size, current_size = CurrentSize}) ->
  if Size >= CurrentSize ->
       From ! ok_msg();
     true ->
       timer:send_after(500, {check, From})
  end,
  {reply, ok, State#state{current_size = CurrentSize + MsgSize}};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(Request, State) ->
  handle_info(Request, State).

handle_info(refresh, State) ->
  {noreply, State#state{current_size = 0}};
handle_info({check, From},
            State = #state{flow_control_size = Size, current_size = CurrentSize}) ->
  if Size >= CurrentSize ->
       From ! ok_msg();
     true ->
       timer:send_after(500, {check, From})
  end,
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{flow_control_timer = Timer}) ->
  _ = timer:cancel(Timer),
  ok.
