%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        url => "http://10.5.0.111:6570,127.0.0.1:6570"
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
        url => "http://10.5.0.111:6570,10.5.0.222:6570"
    },

    {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),
    ?assertEqual(
        {error, no_more_urls_to_connect},
        hstreamdb_client:connect(Client)
    ),
    ok = hstreamdb_client:stop(Client).

t_hidden_metadata(_Config) ->
    AuthToken = <<"dXNlcjpwYXNzd29yZA==">>,
    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        metadata => #{<<"authorization">> => fun() -> AuthToken end}
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

t_metadata(_Config) ->
    AuthToken = <<"dXNlcjpwYXNzd29yZA==">>,

    ClientConfig = #{
        url => "http://127.0.0.1:6570",
        metadata => #{<<"authorization">> => AuthToken}
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
        username => <<"user">>,
        password => <<"password">>
    },

    {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),

    ?assertMatch(
        #{<<"authorization">> := <<"Basic dXNlcjpwYXNzd29yZA==">>},
        hstreamdb_client:metadata(Client)
    ).

t_reaper(_Config) ->
    ClientConfig = #{url => "http://127.0.0.1:6570"},

    {ok, Client} = hstreamdb_client:create(test_c1, ClientConfig),

    _ = application:stop(hstreamdb_erl),
    _ = application:start(hstreamdb_erl),

    %% Reaper should drop all channels on termination
    ?assertError(
        badarg,
        hstreamdb_client:echo(Client)
    ).
