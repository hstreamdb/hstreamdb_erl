%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(hstreamdb_test_helpers).

-compile(export_all).
-compile(nowarn_export_all).

-define(RPC_OPTIONS, #{
    pool_size => 3
}).

-define(CLIENT_OPTIONS, #{
    url => "http://127.0.0.1:6570",
    rpc_options => ?RPC_OPTIONS,
    host_mapping => #{
        {<<"10.5.0.4">>, 6570} => {<<"127.0.0.1">>, 7570},
        {<<"10.5.0.5">>, 6572} => {<<"127.0.0.1">>, 7572}
    }
}).

test_cases(Mod) ->
    [
        F
     || {F, _Ar} <- Mod:module_info(exports),
        string:slice(atom_to_list(F), 0, 2) == "t_"
    ].

default_options() ->
    ?CLIENT_OPTIONS.

client(Name) ->
    client(Name, #{}).

client(Name, Opts) ->
    {ok, Client} = hstreamdb_client:start(Name, maps:merge(?CLIENT_OPTIONS, Opts)),
    Client.

drain_inbox() ->
    receive
        Message ->
            drain_inbox() ++ [Message]
    after 0 ->
        []
    end.

%%-------------------------------------------------------------------------------
%% Toxiproxy utils
%%-------------------------------------------------------------------------------

-define(TOXIPROXY_HOST, "127.0.0.1").
-define(TOXIPROXY_PORT, 8474).

reset_proxy() ->
    Url = "http://" ++ ?TOXIPROXY_HOST ++ ":" ++ integer_to_list(?TOXIPROXY_PORT) ++ "/reset",
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(
        post,
        {Url, [], "application/json", Body},
        [],
        [{body_format, binary}]
    ).

with_failure(FailureType, Name, Fun) ->
    enable_failure(FailureType, Name, ?TOXIPROXY_HOST, ?TOXIPROXY_PORT),
    try
        Fun()
    after
        heal_failure(FailureType, Name, ?TOXIPROXY_HOST, ?TOXIPROXY_PORT)
    end.

enable_failure(FailureType, Name, ProxyHost, ProxyPort) ->
    case FailureType of
        down -> switch_proxy(off, Name, ProxyHost, ProxyPort);
        timeout -> timeout_proxy(on, Name, ProxyHost, ProxyPort);
        latency_up -> latency_up_proxy(on, Name, ProxyHost, ProxyPort)
    end.

heal_failure(FailureType, Name, ProxyHost, ProxyPort) ->
    case FailureType of
        down -> switch_proxy(on, Name, ProxyHost, ProxyPort);
        timeout -> timeout_proxy(off, Name, ProxyHost, ProxyPort);
        latency_up -> latency_up_proxy(off, Name, ProxyHost, ProxyPort)
    end.

switch_proxy(Switch, Name, ProxyHost, ProxyPort) ->
    Url = "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/" ++ Name,
    Body =
        case Switch of
            off -> #{<<"enabled">> => false};
            on -> #{<<"enabled">> => true}
        end,
    BodyBin = emqx_utils_json:encode(Body),
    {ok, {{_, 200, _}, _, _}} = httpc:request(
        post,
        {Url, [], "application/json", BodyBin},
        [],
        [{body_format, binary}]
    ).

timeout_proxy(on, Name, ProxyHost, ProxyPort) ->
    Url =
        "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/" ++ Name ++
            "/toxics",
    NameBin = list_to_binary(Name),
    Body = #{
        <<"name">> => <<NameBin/binary, "_timeout">>,
        <<"type">> => <<"timeout">>,
        <<"stream">> => <<"upstream">>,
        <<"toxicity">> => 1.0,
        <<"attributes">> => #{<<"timeout">> => 0}
    },
    BodyBin = emqx_utils_json:encode(Body),
    {ok, {{_, 200, _}, _, _}} = httpc:request(
        post,
        {Url, [], "application/json", BodyBin},
        [],
        [{body_format, binary}]
    );
timeout_proxy(off, Name, ProxyHost, ProxyPort) ->
    ToxicName = Name ++ "_timeout",
    Url =
        "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/" ++ Name ++
            "/toxics/" ++ ToxicName,
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(
        delete,
        {Url, [], "application/json", Body},
        [],
        [{body_format, binary}]
    ).

latency_up_proxy(on, Name, ProxyHost, ProxyPort) ->
    Url =
        "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/" ++ Name ++
            "/toxics",
    NameBin = list_to_binary(Name),
    Body = #{
        <<"name">> => <<NameBin/binary, "_latency_up">>,
        <<"type">> => <<"latency">>,
        <<"stream">> => <<"upstream">>,
        <<"toxicity">> => 1.0,
        <<"attributes">> => #{
            <<"latency">> => 20_000,
            <<"jitter">> => 3_000
        }
    },
    BodyBin = emqx_utils_json:encode(Body),
    {ok, {{_, 200, _}, _, _}} = httpc:request(
        post,
        {Url, [], "application/json", BodyBin},
        [],
        [{body_format, binary}]
    );
latency_up_proxy(off, Name, ProxyHost, ProxyPort) ->
    ToxicName = Name ++ "_latency_up",
    Url =
        "http://" ++ ProxyHost ++ ":" ++ integer_to_list(ProxyPort) ++ "/proxies/" ++ Name ++
            "/toxics/" ++ ToxicName,
    Body = <<>>,
    {ok, {{_, 204, _}, _, _}} = httpc:request(
        delete,
        {Url, [], "application/json", Body},
        [],
        [{body_format, binary}]
    ).
