-module(hstreamdb_flow_controller).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([ok_msg/0]).

-record(state,
        {flow_control_timer :: timer:tref(),
         resend_interval,
         flow_control_traffic_size,
         current_traffic_size,
         flow_control_memory,
         current_memory_size}).

ok_msg() ->
  {flow_control, ok}.

init(Options) ->
  Interval = proplists:get_value(flow_control_interval, Options, 1000),
  ResendInterval = proplists:get_value(resend_interval, Options, 1000),
  Size = proplists:get_value(flow_control_traffic_size, Options),
  MemSize = proplists:get_value(flow_control_memory, Options),
  CurMemSize = proplists:get_value(total, erlang:memory()),
  {ok,
   #state{flow_control_timer = timer:send_interval(Interval, refresh),
          resend_interval = ResendInterval,
          flow_control_traffic_size = Size,
          flow_control_memory = MemSize,
          current_traffic_size = CurMemSize}}.

handle_call({check, MsgSize},
            {From, _},
            State =
              #state{flow_control_traffic_size = TrafficSize,
                     resend_interval = ResendInterval,
                     current_traffic_size = CurTrafficSize,
                     flow_control_memory = MemSize,
                     current_memory_size = CurMemSize}) ->
  do_check_and_send(TrafficSize, CurTrafficSize, MemSize, CurMemSize, From, ResendInterval),
  {reply, ok, State#state{current_traffic_size = CurTrafficSize + MsgSize}};
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

handle_cast(Request, State) ->
  handle_info(Request, State).

handle_info({check, From},
            State =
              #state{flow_control_traffic_size = Size,
                     current_traffic_size = CurSize,
                     resend_interval = ResendInterval,
                     flow_control_memory = MemSize,
                     current_memory_size = CurMemSize}) ->
  do_check_and_send(Size, CurSize, MemSize, CurMemSize, From, ResendInterval),
  {noreply, State};
handle_info(refresh, State) ->
  CurMemSize = proplists:get_value(total, erlang:memory()),
  {noreply, State#state{current_memory_size = CurMemSize, current_traffic_size = 0}};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{flow_control_timer = Timer}) ->
  _ = timer:cancel(Timer),
  ok.

do_check_and_send(Size, CurSize, MemSize, CurMemSize, From, ResendInterval) ->
  case MemSize > CurMemSize andalso Size > CurSize of
    true ->
      From ! ok_msg();
    false ->
      timer:send_after(ResendInterval, {check, From})
  end.
