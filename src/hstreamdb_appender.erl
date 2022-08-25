-module(hstreamdb_appender).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {channel_manager, byte_size_ref}).

init(Options) ->
  ByteSizeRef = proplists:get_value(byte_size_ref, Options, undefined),
  {ok,
   #state{channel_manager = hstreamdb_channel_mgr:start(Options),
          byte_size_ref = ByteSizeRef}}.

handle_call(Request, _From, State) ->
  {noreply, NState} = handle_cast(Request, State),
  {reply, ok, NState}.

handle_cast({append, {Stream, OrderingKey, Records}},
            State = #state{channel_manager = ChannelM, byte_size_ref = ByteSizeRef}) ->
  case hstreamdb_channel_mgr:lookup_channel(OrderingKey, ChannelM) of
    {ok, Channel} ->
      case call_rpc_append(Stream, Records, Channel, ByteSizeRef) of
        {ok, _} ->
          {noreply, State};
        {error, _} ->
          NChannelM = hstreamdb_channel_mgr:bad_channel(OrderingKey, ChannelM),
          {noreply, State#state{channel_manager = NChannelM}}
      end;
    {ok, Channel, NChannelM} ->
      case call_rpc_append(Stream, Records, Channel, ByteSizeRef) of
        {ok, _} ->
          {noreply, State#state{channel_manager = NChannelM}};
        {error, _} ->
          ErrNChannelM = hstreamdb_channel_mgr:bad_channel(OrderingKey, ChannelM),
          {noreply, State#state{channel_manager = ErrNChannelM}}
      end;
    {error, Error} ->
      {stop, Error, State}
  end.

handle_info(Request, State) ->
  case Request of
    {append, {_Stream, _OrderingKey, _Records}} ->
      handle_cast(Request, State);
    _ ->
      {noreply, State}
  end.

terminate(_Reason, #state{channel_manager = ChannelM}) ->
  hstreamdb_channel_mgr:stop(ChannelM).

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

call_rpc_append(Stream, Records, Channel, ByteSizeRef) ->
  Request = #{streamName => Stream, records => Records},
  Options = #{channel => Channel},
  case hstreamdb_client:append(Request, Options) of
    {ok, AppendResponse, _MetaData} ->
      Size =
        lists:foldl(fun(X, Sum) ->
                       erlang:byte_size(
                         maps:get(payload, X))
                       + Sum
                    end,
                    0,
                    Records),
      atomics:add(ByteSizeRef, 1, Size),
      io:format("[DEBUG]: append RPC~n"),
      {ok, AppendResponse};
    {error, AppendErrorReason} ->
      io:format("[ERROR]: append error ~p~n", [AppendErrorReason]),
      {error, AppendErrorReason}
  end.
