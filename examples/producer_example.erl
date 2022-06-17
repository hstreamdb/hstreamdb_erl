-module(producer_example).

-export([ start/0
        , callback/1
        ]).

start() ->
    %% producer_example:start().
    _ = application:ensure_all_started(hstreamdb_erl),
    ClientOptions = [
        {url,  "http://192.168.1.234:6570"},
        {rpc_options, #{pool_size => 8}}
    ],
    {ok, Client} = hstreamdb:start_client(test_c, ClientOptions),
    io:format("start client  ~p~n", [Client]),
    Start2 = hstreamdb:start_client(test_c, ClientOptions),
    io:format("start client2  ~p~n", [Start2]),

    StopClient = hstreamdb:stop_client(test_c),
    io:format("stop client ~p~n", [StopClient]),

    Start3 = hstreamdb:start_client(test_c, ClientOptions),
    io:format("start client3  ~p~n", [Start3]),

    Echo = hstreamdb:echo(Client),
    io:format("echo  ~p~n", [Echo]),
    ProducerOptions = [
        {stream, "stream2"},
        {callback, {?MODULE, callback}},
        {max_records, 1000},
        {interval, 1000}
    ],
    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),
    io:format("start producer  ~p~n", [Producer]),

    OrderingKey = "ok1",
    PayloadType = raw,
    Payload = <<"data 12">>,
    Record1 = hstreamdb:to_record(OrderingKey, PayloadType, Payload),
    io:format("to record ~p~n", [Record1]),
    Append = hstreamdb:append(Producer, Record1),
    % Append = [hstreamdb:append(Producer, Record1) || _ <- lists:seq(0, 100)],
    io:format("append ~p~n", [Append]),

    timer:sleep(2000),

    AppendFlush = hstreamdb:append_flush(Producer, Record1),
    io:format("append flush ~p~n", [AppendFlush]),
    
    timer:sleep(1000),
    Stop = hstreamdb:stop_producer(Producer),
    io:format("stop producer  ~p~n", [Stop]),

    AppendAfterStop = hstreamdb:append(Producer, Record1),
    io:format("append after stop ~p~n", [AppendAfterStop]),
    ok.

callback(A) ->
    io:format("callback ~p~n", [A]).
