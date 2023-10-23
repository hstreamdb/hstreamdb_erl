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

t_connect_next(_Config) ->
    ClientConfig = #{
        url => "http://10.5.0.111:6570,127.0.0.1:6570"
    },

    {ok, Client0} = hstreamdb_client:create(test_c1, ClientConfig),
    {ok, Client1} = hstreamdb_client:connect_next(Client0),

    ?assertMatch(
        {error, _},
        hstreamdb_client:echo(Client1)
    ),

    ok = hstreamdb_client:stop(Client1),
    {ok, Client2} = hstreamdb_client:connect_next(Client1),

    ?assertMatch(
        ok,
        hstreamdb_client:echo(Client2)
    ).
