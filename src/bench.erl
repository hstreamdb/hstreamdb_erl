-module(bench).

-define(PAYLOAD_4K,
        <<"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"
          "7DRGj83OiVZH1X2W">>).

-export([start/0, cb/1]).

-define(URL, "http://127.0.0.1:6570").

start() ->
  ByteSizeRef = new_byte_size_ref(),
  PoolSize = 8,
  ClientName = test_c,
  {ok, Client} = hstreamdb:start_client(ClientName, client_opts(PoolSize)),
  Streams =
    lists:map(fun(Ix) ->
                 Stream =
                   str_fmt("stream___~p__~p__~p__", [Ix, erlang:system_time(), rand:uniform(200)]),
                 _ = create_stream(ClientName, Stream),
                 Stream
              end,
              seq_list(20)),
  Producers =
    lists:map(fun(Stream) ->
                 {ok, Producer} =
                   hstreamdb:start_producer(Client,
                                            atom_fmt("producer___~p~p~p___",
                                                     [rand:uniform(200),
                                                      rand:uniform(200),
                                                      rand:uniform(200)]),
                                            producer_opts(Stream, ByteSizeRef)),
                 Producer
              end,
              Streams),
  producer_list(Producers, 12),
  error_return.

cb(_) ->
  ok.

new_byte_size_ref() ->
  ByteSizeRef = atomics:new(1, [{signed, false}]),
  gen_server:start(cnt, [ByteSizeRef], []),
  ByteSizeRef.

producer_list(Producers, OrdKeyNum) ->
  [X | XS] = Producers,
  lists:foreach(fun(Producer) -> spawn_loop_write(Producer, 4, rand_key(OrdKeyNum)) end,
                XS),
  loop_write(X, 4, rand_key(OrdKeyNum)).

spawn_loop_write(Producer, Size, OrdKey) ->
  spawn(fun() -> loop_write(Producer, Size, OrdKey) end),
  spawn(fun() -> loop_write(Producer, Size, OrdKey) end),
  spawn(fun() -> loop_write(Producer, Size, OrdKey) end),
  spawn(fun() -> loop_write(Producer, Size, OrdKey) end).

loop_write(Producer, Size, OrdKey) ->
  hstreamdb:append(Producer, OrdKey, raw, rand_bytes(Size, k)),
  loop_write(Producer, Size, OrdKey).

client_opts(PoolSize) ->
  RPCOptions = #{pool_size => PoolSize},
  [{url, ?URL}, {rpc_options, RPCOptions}].

producer_opts(StreamName, ByteSizeRef) ->
  [{stream, StreamName},
   {callback, {?MODULE, cb}},
   {max_records, 250},
   {interval, 1000},
   {byte_size_ref, ByteSizeRef},
   {flow_control_interval, 1000},
   {flow_control_size, 100 * 1000 * 1000},
   {flow_control_memory, 2 * 1000 * 1000 * 1000},
   {compression_type, zstd}].

seq_list(Size) ->
  lists:seq(1, Size).

% rand_bytes(N) ->
%   crypto:strong_rand_bytes(N).

% rand_bytes(N, k) ->
%   rand_bytes(N * 1000).

rand_bytes(_, _) ->
  ?PAYLOAD_4K.

str_fmt(Fmt, Args) ->
  lists:flatten(
    io_lib:format(Fmt, Args)).

atom_fmt(Fmt, Args) ->
  erlang:list_to_atom(str_fmt(Fmt, Args)).

rand_key(Size) when Size == 1 ->
  rand_key();
rand_key(Size) ->
  str_fmt("ordKey___~p___", [rand:uniform(Size)]).

rand_key() ->
  "".

create_stream(ClientName, Stream) when is_atom(ClientName) ->
  create_stream(erlang:atom_to_list(ClientName), Stream);
create_stream(ClientName, Stream) when is_list(Stream) and is_list(ClientName) ->
  hstreamdb_client:create_stream(#{streamName => Stream,
                                   replicationFactor => 1,
                                   backlogDuration => 60 * 30},
                                 #{channel => ClientName}).
