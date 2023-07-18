-module(hstreamdb_sandbox).

%% $ ./rebar3 as dev shell

-include("hstreamdb.hrl").

-export([
    start/0,
    client/0,
    test_lookup_resourse/0,
    populate_stream/1,
    producer/2,
    callback/1,
    read_stream/1,
    read_stream1/1,
    read_stream2/1
]).

client() ->
    Name = "c-" ++ integer_to_list(erlang:unique_integer([positive])),
    {ok, Client} = hstreamdb_client:start(Name, [
        {url, "http://127.0.0.1:6570"},
        {rpc_options, #{
            pool_size => 3
            % gun_opts => #{
            %     transport => tls,
            %     transport_opts => [{cacertfile, CA}]
            % }
        }},
        {host_mapping, #{
            <<"10.5.0.4">> => <<"127.0.0.1">>,
            <<"10.5.0.5">> => <<"127.0.0.1">>
        }}
    ]),
    io:format("start client  ~p~n", [Client]),
    Client.

test_lookup_resourse() ->
    Client = client(),
    do_n(
        50,
        fun(N) ->
            Stream = "tls_" ++ integer_to_list(N),
            _ = hstreamdb_client:create_stream(Client, Stream, 2, 24 * 60 * 60, 1),
            Shards = hstreamdb_client:list_shards(Client, Stream),
            {ok, [#{shardId := ShardId}]} = Shards,
            {ok, {Host0, Port0}} = hstreamdb_client:lookup_shard(Client, ShardId),

            {ok, {Host1, Port1}} = hstreamdb_client:lookup_resource(Client, ?RES_STREAM, Stream),

            io:format("lookup shard ~p, Shard: ~p ~p:~p vs ~p:~p~n", [
                Stream, ShardId, Host0, Port0, Host1, Port1
            ])
        end
    ).

populate_stream(Stream) ->
    Client = client(),
    _ = hstreamdb_client:create_stream(Client, Stream, 2, 24 * 60 * 60, 1),
    producer(populate, Stream),

    do_n(
        1000,
        fun(N) ->
            PartitioningKey = "PK",
            PayloadType = raw,
            Payload = <<"hello-", (integer_to_binary(N))/binary>>,
            Record = hstreamdb:to_record(PartitioningKey, PayloadType, Payload),
            _ = hstreamdb:append(populate, Record)
        end
    ),
    ok = hstreamdb:stop_producer(populate).

read_stream(Stream) ->
    Client = client(),
    Reader = hstreamdb:start_client_manager(Client),
    hstreamdb:read_single_shard_stream(Reader, Stream).

read_stream1(Stream) ->
    Client = client(),
    Reader = hstreamdb:start_client_manager(Client),
    hstreamdb:read_single_shard_stream(Reader, Stream, #{
        limits => #{
            from => #{offset => {specialOffset, 0}},
            until => #{
                offset => {timestampOffset, #{timestampInMs => erlang:system_time(millisecond)}}
            },
            max_read_batches => 100
        }
    }).

read_stream2(Stream) ->
    Client = client(),
    Reader = hstreamdb:start_client_manager(Client),
    hstreamdb:read_single_shard_stream(Reader, Stream, #{
        limits => #{
            from => #{offset => {specialOffset, 0}},
            until => #{
                offset =>
                    {recordOffset, #{
                        batchId => 4294967300,
                        batchIndex => 385,
                        shardId => 1877093414140935
                    }}
            },
            max_read_batches => 100
        }
    }).

producer(Producer, Stream) ->
    Client = client(),
    ProducerOptions = [
        {pool_size, 4},
        {stream, Stream},
        {callback, {producer_example, callback}},
        {max_records, 1000},
        {interval, 1000}
    ],
    ok = hstreamdb:start_producer(Client, Producer, ProducerOptions).

start() ->
    Stream = "demo4",

    _ = application:ensure_all_started(hstreamdb_erl),

    Client = client(),
    Echo = hstreamdb_client:echo(Client),
    io:format("echo: ~p~n", [Echo]),

    CreateStream = hstreamdb_client:create_stream(Client, Stream, 2, 24 * 60 * 60, 1),
    io:format("create_stream: ~p~n", [CreateStream]),

    ProducerOptions = [
        {pool_size, 4},
        {stream, Stream},
        {callback, {producer_example, callback}},
        {max_records, 1000},
        {interval, 1000}
    ],
    Producer = test_producer,
    ok = hstreamdb:start_producer(Client, Producer, ProducerOptions),
    io:format("start producer  ~p~n", [Producer]),

    PartitioningKey = "ok1",
    PayloadType = raw,
    Payload = <<"hello stream !">>,
    Record = hstreamdb:to_record(PartitioningKey, PayloadType, Payload),
    io:format("to record ~p~n", [Record]),

    do_n(
        1000,
        fun() ->
            _ = hstreamdb:append(Producer, Record)
        end
    ),

    % timer:sleep(1000),
    Stop = hstreamdb:stop_producer(Producer),
    io:format("stop producer  ~p~n", [Stop]),

    Client.

callback(_A) ->
    % io:format("callback ~p~n", [A]).
    ok.

do_n(N, _Fun) when N =< 0 -> ok;
do_n(N, Fun) when is_function(Fun, 0) ->
    _ = Fun(),
    do_n(N - 1, Fun);
do_n(N, Fun) when is_function(Fun, 1) ->
    _ = Fun(N),
    do_n(N - 1, Fun).
