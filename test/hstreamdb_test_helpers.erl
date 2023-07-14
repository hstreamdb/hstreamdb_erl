-module(hstreamdb_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-define(RPC_OPTIONS, #{
    pool_size => 3
}).

-define(CLIENT_OPTIONS, [
    {url, "http://127.0.0.1:6570"},
    {rpc_options, ?RPC_OPTIONS},
    {host_mapping, #{
        <<"10.5.0.4">> => <<"127.0.0.1">>,
        <<"10.5.0.5">> => <<"127.0.0.1">>
    }}
]).

-define(CONN_ATTEMPTS, 60).

test_cases(Mod) ->
    [
        F
     || {F, _Ar} <- Mod:module_info(exports),
        string:slice(atom_to_list(F), 0, 2) == "t_"
    ].

default_options() ->
    ?CLIENT_OPTIONS.

client(Name) ->
    client(Name, ?CONN_ATTEMPTS).

client(_Name, N) when N =< 0 -> error(cannot_connect);
client(Name, N) ->
    try
        _ = hstreamdb_client:stop(Name),
        {ok, Client} = hstreamdb_client:start(Name, ?CLIENT_OPTIONS),
        ok = hstreamdb_client:echo(Client),
        Client
    catch
        Class:Error:Stack ->
            ct:print("Error connecting: ~p~n~p", [{Class, Error}, Stack]),
            ct:sleep(timer:seconds(1)),
            client(Name, N - 1)
    end.
