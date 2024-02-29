%%--------------------------------------------------------------------
%% Copyright (c) 2020-202 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(hstreamdb_client_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include("assert.hrl").

all() ->
    hstreamdb_test_helpers:test_cases(?MODULE).

init_per_suite(Config) ->
    application:ensure_all_started(hstreamdb_erl),
    Config.
end_per_suite(_Config) ->
    application:stop(hstreamdb_erl),
    ok.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_connect(_Config) ->
    ClientConfig = #{
        url => "http://10.5.0.111:6570,127.0.0.1:6570",
        password => <<"hstream">>,
        username => <<"hstream">>
    },

    lists:foreach(
        fun(_) ->
            {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),
            ?assertEqual(
                ok,
                hstreamdb_client:connect(Client)
            ),
            ok = hstreamdb_client:stop(Client)
        end,
        lists:seq(1, 10)
    ).

t_connect_fail(_Config) ->
    ClientConfig = #{
        url => "http://10.5.0.111:6570,10.5.0.222:6570",
        password => <<"hstream">>,
        username => <<"hstream">>
    },

    {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),
    ?assertEqual(
        {error, no_more_urls_to_connect},
        hstreamdb_client:connect(Client)
    ),
    ok = hstreamdb_client:stop(Client).

t_hidden_metadata(_Config) ->
    Auth = <<"Basic aHN0cmVhbTpoc3RyZWFt">>,
    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        metadata => #{<<"authorization">> => fun() -> Auth end}
    },

    {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),
    ?assertMatch(
        #{<<"authorization">> := Auth},
        hstreamdb_client:metadata(Client)
    ).

t_metadata(_Config) ->
    Auth = <<"Basic aHN0cmVhbTpoc3RyZWFt">>,

    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        metadata => #{<<"authorization">> => Auth}
    },

    {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),
    ?assertEqual(
        ok,
        hstreamdb_client:connect(Client)
    ),
    ?assertEqual(
        ok,
        hstreamdb_client:stop(Client)
    ).

t_auth(_Config) ->
    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        username => <<"hstream">>,
        password => <<"hstream">>
    },

    {ok, Client0} = hstreamdb_client:create(test_c1, ClientConfig),

    ?assertMatch(
        #{<<"authorization">> := <<"Basic aHN0cmVhbTpoc3RyZWFt">>},
        hstreamdb_client:metadata(Client0)
    ),

    {ok, Client1} = hstreamdb_client:connect(Client0, <<"127.0.0.1">>, 6570),

    ?assertMatch(
        #{<<"authorization">> := <<"Basic aHN0cmVhbTpoc3RyZWFt">>},
        hstreamdb_client:metadata(Client1)
    ).

t_reap_sup_died(_Config) ->
    Name = ?FUNCTION_NAME,
    CasePid = self(),
    Pid = spawn(fun() ->
        {ok, Pid} = hstreamdb_grpc_sup:start_link(Name),
        CasePid ! {sup_ref, Pid},
        receive
            stop -> ok
        end
    end),

    SupRef =
        receive
            {sup_ref, Ref} -> Ref
        end,

    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        username => <<"hstream">>,
        password => <<"hstream">>,
        sup_ref => SupRef,
        reap_channel => true
    },

    {ok, Client} = hstreamdb_client:start(test_c1, ClientConfig),

    exit(Pid, kill),

    ct:sleep(100),

    ?assertError(
        badarg,
        hstreamdb_client:echo(Client)
    ).

t_reap_owner_died(_Config) ->
    Name = ?FUNCTION_NAME,
    {ok, SupRef} = hstreamdb_grpc_sup:start_link(Name),

    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        username => <<"hstream">>,
        password => <<"hstream">>,
        sup_ref => SupRef,
        reap_channel => true
    },

    CasePid = self(),

    Pid = spawn_link(
        fun() ->
            {ok, Client} = hstreamdb_client:start(test_c1, ClientConfig),
            CasePid ! {client, Client},
            receive
                stop -> ok
            end
        end
    ),

    Client =
        receive
            {client, C} -> C
        end,

    ?assertMatch(
        ok,
        hstreamdb_client:echo(Client)
    ),

    Pid ! stop,

    ct:sleep(100),

    ?assertError(
        badarg,
        hstreamdb_client:echo(Client)
    ),

    ?assertMatch(
        ok,
        hstreamdb_client:reconnect(Client)
    ),

    ?assertMatch(
        ok,
        hstreamdb_client:echo(Client)
    ).
