-module(hstreamdb_appender).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-type compression_type() :: none | gzip | zstd.

-record(state, {channel_manager, callback, compression_type :: compression_type()}).

init(Options) ->
  CompressionType = proplists:get_value(compression_type, Options, gzip),
  Callback = proplists:get_value(callback, Options),
  {ok,
   #state{channel_manager = hstreamdb_channel_mgr:start(Options),
          callback = Callback,
          compression_type = CompressionType}}.

handle_call(Request, _From, State) ->
  {noreply, NState} = handle_cast(Request, State),
  {reply, ok, NState}.

handle_cast({append, {Stream, OrderingKey, Records}},
            State =
              #state{channel_manager = ChannelM,
                     callback = Callback,
                     compression_type = CompressionType}) ->
  case hstreamdb_channel_mgr:lookup_channel(OrderingKey, ChannelM) of
    {ok, Channel} ->
      RpcResp = call_rpc_append(Stream, OrderingKey, Records, Channel, CompressionType),
      _ = apply_callback(Callback, {{flush, Stream, Records}, RpcResp}),
      case RpcResp of
        {ok, _} ->
          {noreply, State};
        {error, _} ->
          NChannelM = hstreamdb_channel_mgr:bad_channel(OrderingKey, ChannelM),
          {noreply, State#state{channel_manager = NChannelM}}
      end;
    {ok, Channel, NChannelM} ->
      RpcResp = call_rpc_append(Stream, OrderingKey, Records, Channel, CompressionType),
      _ = apply_callback(Callback, {{flush, Stream, Records}, RpcResp}),
      case RpcResp of
        {ok, _} ->
          {noreply, State#state{channel_manager = NChannelM}};
        {error, _} ->
          ErrNChannelM = hstreamdb_channel_mgr:bad_channel(OrderingKey, ChannelM),
          {noreply, State#state{channel_manager = ErrNChannelM}}
      end;
    {error, Error} ->
      _ = apply_callback(Callback, {{flush, Stream, Records}, {error, Error}}),
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

call_rpc_append(Stream, OrderingKey, Records, Channel, CompressionType) ->
  BatchHStreamRecords = #{records => Records},
  Payload = hstreamdb_api:encode_msg(BatchHStreamRecords, batch_h_stream_records, []),
  case compress_payload(Payload, CompressionType) of
    {ok, CompressedPayload} ->
      BatchedRecord =
        #{compressionType => compression_type_to_enum(CompressionType),
          batchSize => length(Records),
          orderingKey => OrderingKey,
          payload => CompressedPayload},
      Request = #{streamName => Stream, records => BatchedRecord},
      Options = #{channel => Channel},
      case hstreamdb_client:append(Request, Options) of
        {ok, AppendResponse, _MetaData} ->
          {ok, AppendResponse};
        {error, AppendErrorReason} ->
          {error, AppendErrorReason}
      end;
    {error, R} ->
      {error, R}
  end.

-spec compress_payload(Payload :: binary(), CompressionType :: compression_type()) ->
                        {ok, binary()} | {error, any()}.
compress_payload(Payload, CompressionType) ->
  case CompressionType of
    none ->
      {ok, Payload};
    gzip ->
      {ok, zlib:gzip(Payload)};
    zstd ->
      case ezstd:compress(Payload) of
        {error, R} ->
          {error, R};
        R ->
          {ok, R}
      end
  end.

compression_type_to_enum(CompressionType) ->
  case CompressionType of
    none ->
      0;
    gzip ->
      1;
    zstd ->
      2
  end.

apply_callback({M, F}, R) ->
  erlang:apply(M, F, [R]);
apply_callback({M, F, A}, R) ->
  erlang:apply(M, F, [R | A]);
apply_callback(F, R) ->
  F(R).
