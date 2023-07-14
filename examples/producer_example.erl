-module(producer_example).

%% $ ./rebar3 shell
%% > c("examples/producer_example").
%% > producer_example:start().

-export([
    start/0,
    callback/1
]).

start() ->
    _ = application:ensure_all_started(hstreamdb_erl),
    RPCOptions = #{
        pool_size => 3
        % gun_opts => #{
        %     transport => tls,
        %     transport_opts => [{cacertfile, CA}]
        % }
    },
    ClientOptions = [
        {url, "http://127.0.0.1:6570"},
        {rpc_options, RPCOptions},
        {host_mapping, #{
            <<"10.5.0.4">> => <<"127.0.0.1">>,
            <<"10.5.0.5">> => <<"127.0.0.1">>
        }}
    ],
    {ok, Client} = hstreamdb_client:start(test_c, ClientOptions),
    io:format("start client  ~p~n", [Client]),

    Echo = hstreamdb_client:echo(Client),
    io:format("echo: ~p~n", [Echo]),

    CreateStream = hstreamdb_client:create_stream(Client, "demo2", 2, 24 * 60 * 60, 2),
    io:format("create_stream: ~p~n", [CreateStream]),

    ProducerOptions = [
        {pool_size, 4},
        {stream, "demo2"},
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
    Record1 = hstreamdb:to_record(PartitioningKey, PayloadType, Payload),
    Record2 = hstreamdb:to_record(PartitioningKey, PayloadType, <<"batch 1">>),
    io:format("to record ~p~n", [Record1]),

    do_n(
        1000,
        fun() ->
            _Append1 = hstreamdb:append(Producer, Record1),
            _Append2 = hstreamdb:append(Producer, Record2)
        end
    ),

    timer:sleep(3000),

    io:format("start append flush ~n"),

    AppendFlushSingle = hstreamdb:append_flush(Producer, Record1),
    io:format("append flush AppendFlushSingle ~p~n", [AppendFlushSingle]),

    timer:sleep(1000),
    Stop = hstreamdb:stop_producer(Producer),
    io:format("stop producer  ~p~n", [Stop]),

    timer:sleep(200),
    RestartProducer = hstreamdb:start_producer(Client, test_producer, ProducerOptions),
    io:format("restart producer  ~p~n", [RestartProducer]),

    StopClient = hstreamdb_client:stop(Client),
    io:format("stop client ~p~n", [StopClient]),

    ok.

callback(A) ->
    io:format("callback ~p~n", [A]).

do_n(N, _Fun) when N =< 0 -> ok;
do_n(N, Fun) ->
    _ = Fun(),
    do_n(N - 1, Fun).
