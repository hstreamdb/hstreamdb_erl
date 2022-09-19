-module(producer_example).

-export([ start/0
        , callback/1
        ]).

start() ->
    %% producer_example:start().
    _ = application:ensure_all_started(hstreamdb_erl),
    RPCOptions = #{
        pool_size => 3
        % gun_opts => #{
        %     transport => ssl,
        %     transport_opts => [{cacertfile, CA}]
        % }
    },
    ClientOptions = [
        % {url,  "http://119.3.80.172:6570"},
        {url,  "http://127.0.0.1:6570"},
        {rpc_options, RPCOptions},
        {host_mapping, #{<<"10.5.0.4">> => <<"127.0.0.1">>,
                         <<"10.5.0.5">> => <<"127.0.0.1">>}}
    ],
    {ok, Client} = hstreamdb:start_client(test_c, ClientOptions),
    io:format("start client  ~p~n", [Client]),
    % Start2 = hstreamdb:start_client(test_c, ClientOptions),
    % io:format("start client2  ~p~n", [Start2]),

    % StopClient = hstreamdb:stop_client(test_c),
    % io:format("stop client ~p~n", [StopClient]),

    % Start3 = hstreamdb:start_client(test_c, ClientOptions),
    % io:format("start client3  ~p~n", [Start3]),

    Echo = hstreamdb:echo(Client),
    io:format("echo: ~p~n", [Echo]),
    
    CreateStream = hstreamdb:create_stream( Client, "demo2", 2, 24*60*60, 2),
    io:format("create_stream: ~p~n", [CreateStream]),

    ProducerOptions = [
        {pool_size, 4},
        {stream, "demo2"},
        {callback, {producer_example, callback}},
        {max_records, 1000},
        {interval, 1000}
    ],
    {ok, Producer} = hstreamdb:start_producer(Client, test_producer, ProducerOptions),
    io:format("start producer  ~p~n", [Producer]),

    PartitioningKey = "ok1",
    PayloadType = raw,
    Payload = <<"hello stream !">>,
    Record1 = hstreamdb:to_record(PartitioningKey, PayloadType, Payload),
    Record2 = hstreamdb:to_record(PartitioningKey, PayloadType, <<"batch 1">>),
    io:format("to record ~p~n", [Record1]),

    do_n(1000,
         fun() ->
                 _Append1 = hstreamdb:append(Producer, Record1),
                 _Append2 = hstreamdb:append(Producer, Record2)
         end),
    % Append = [
    %     begin
    %         RecordN = hstreamdb:to_record(PartitioningKey, PayloadType, list_to_binary(io_lib:format("message ~p", [N]))),
    %         hstreamdb:append(Producer, RecordN)
    %     end || N <- lists:seq(0, 100)],
    % io:format("append1 ~p~n", [Append1]),
    % io:format("append2 ~p~n", [Append2]),

    timer:sleep(3000),

    io:format("start append flush ~n"),

    {BatchK, R1} = hstreamdb:to_record(PartitioningKey, PayloadType, Payload),
    {BatchK, R2} = hstreamdb:to_record(PartitioningKey, PayloadType, <<"batch 1">>),

    AppendFlushSingle = hstreamdb:append_flush(Producer, Record1),
    AppendFlush = hstreamdb:append_flush(Producer, {BatchK, [R1, R2]}),
    io:format("append flush AppendFlushSingle ~p~n", [AppendFlushSingle]),
    io:format("append flush ~p~n", [AppendFlush]),
    
    timer:sleep(1000),
    Stop = hstreamdb:stop_producer(Producer),
    io:format("stop producer  ~p~n", [Stop]),
    % Stop2 = hstreamdb:stop_producer(Producer),
    % io:format("stop2 producer  ~p~n", [Stop2]),

    timer:sleep(200),
    RestartProducer = hstreamdb:start_producer(Client, test_producer, ProducerOptions),
    io:format("restart producer  ~p~n", [RestartProducer]),

    StopClient = hstreamdb:stop_client(Client),
    io:format("stop client ~p~n", [StopClient]),

    % AppendAfterStop = hstreamdb:append(Producer, Record1),
    % io:format("append after stop ~p~n", [AppendAfterStop]),
    ok.

callback(A) ->
    io:format("callback ~p~n", [A]).

do_n(N, _Fun) when N =< 0 -> ok;
do_n(N, Fun) ->
    _ = Fun(),
    do_n(N - 1, Fun).
