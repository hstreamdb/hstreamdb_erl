-module(bench).

-export([start/0, cb/1]).

-define(URL, "http://127.0.0.1:6570").

start() ->
  ByteSizeRef = new_byte_size_ref(),
  PoolSize = 32,
  ClientName = test_c,
  {ok, Client} = hstreamdb:start_client(ClientName, client_opts(PoolSize)),
  Streams =
    lists:map(fun(Ix) ->
                 Stream =
                   str_fmt("stream___~p__~p__~p__", [Ix, erlang:system_time(), rand:uniform(200)]),
                 _ = create_stream(ClientName, Stream),
                 Stream
              end,
              seq_list(1)),
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
  producer_list(Producers, 400),
  error_return.

cb(_) ->
  ok.

new_byte_size_ref() ->
  ByteSizeRef = atomics:new(1, [{signed, false}]),
  gen_server:start(cnt, [ByteSizeRef], []),
  ByteSizeRef.

producer_list(Producers, OrdKeyNum) ->
  [X | XS] = Producers,
  lists:foreach(fun(Producer) -> spawn_loop_write(Producer, 4, OrdKeyNum) end,
                XS),
  spawn_loop_write(X, 4, OrdKeyNum),
  loop_write(X, 4, OrdKeyNum).

spawn_loop_write(Producer, Size, OrdKeyNum) ->
  lists:foreach(fun(_) ->
                   io:format("spawn"),
                   spawn(fun() ->
                            loop_write(Producer, Size, OrdKeyNum),
                            io:format("error")
                         end)
                end,
                seq_list(200)).

loop_write(Producer, Size, OrdKeyNum) ->
  hstreamdb:append(Producer, rand_key(OrdKeyNum), raw, rand_bytes(Size, k)),
  loop_write(Producer, Size, OrdKeyNum).

client_opts(PoolSize) ->
  RPCOptions = #{pool_size => PoolSize},
  [{url, ?URL}, {rpc_options, RPCOptions}].

producer_opts(StreamName, ByteSizeRef) ->
  [{stream, StreamName},
   {callback, {?MODULE, cb}},
   {max_records, 16},
   {interval, 1000},
   {byte_size_ref, ByteSizeRef},
   {flow_control_interval, 1000},
   {flow_control_size, 100 * 1000 * 1000},
   {flow_control_memory, 2 * 1000 * 1000 * 1000},
   {compression_type, zstd}].

seq_list(Size) ->
  lists:seq(1, Size).

rand_bytes(N) ->
  X = gen_rnd(N, "abcdefghijklmnopqrstuvwxyz1234567890"),
  erlang:list_to_binary(X).

gen_rnd(Length, AllowedChars) ->
  MaxLength = length(AllowedChars),
  lists:foldl(fun(_, Acc) ->
                 [lists:nth(
                    rand:uniform(MaxLength), AllowedChars)]
                 ++ Acc
              end,
              [],
              lists:seq(1, Length)).

rand_bytes(N, k) ->
  rand_bytes(N * 1000).

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
