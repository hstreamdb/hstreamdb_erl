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

-include_lib("kernel/include/logger.hrl").

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
    },
    username => <<"hstream">>,
    password => <<"hstream">>
}).

test_cases(Mod) ->
    [
        F
     || {F, _Ar} <- Mod:module_info(exports),
        string:slice(atom_to_list(F), 0, 2) == "t_"
    ].

default_client_options() ->
    ?CLIENT_OPTIONS.

client_options(ClientOptions) ->
    maps:merge(?CLIENT_OPTIONS, ClientOptions).

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

with_failures([{FailureType, Name} | Rest], Fun) ->
    enable_failure(FailureType, Name, ?TOXIPROXY_HOST, ?TOXIPROXY_PORT),
    try
        with_failures(Rest, Fun)
    after
        heal_failure(FailureType, Name, ?TOXIPROXY_HOST, ?TOXIPROXY_PORT)
    end;
with_failures([], Fun) ->
    Fun().

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
    BodyBin = json_encode(Body),
    ?LOG_DEBUG("[hstreamdb] switch_proxy, url: ~p, body: ~p~n", [Url, BodyBin]),
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
    BodyBin = json_encode(Body),
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
    BodyBin = json_encode(Body),
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

json_encode(Json) ->
    jiffy:encode(Json).

start_disruptor(Hosts, Errors, ErrorPeriodBase, OkPeriodBase) ->
    spawn_link(
        fun() ->
            reset_proxy(),
            loop_disrupt(Hosts, Errors, ErrorPeriodBase, OkPeriodBase)
        end
    ).

stop_disruptor(Disruptor) ->
    Disruptor ! stop.

loop_disrupt(Hosts, Errors, ErrorPeriodBase, OkPeriodBase) ->
    receive
        stop ->
            ok
    after 0 ->
        Host = random_element(Hosts),
        Error = random_element(Errors),
        ErrorPeriod = random_period(ErrorPeriodBase),
        OkPeriod = random_period(OkPeriodBase),
        ?LOG_DEBUG("disrupt: ~p, error: ~p, error_period: ~p, ok_period: ~p~n", [
            Host,
            Error,
            ErrorPeriod,
            OkPeriod
        ]),
        with_failure(Error, Host, fun() -> timer:sleep(ErrorPeriod) end),
        timer:sleep(OkPeriod),
        loop_disrupt(Hosts, Errors, ErrorPeriodBase, OkPeriodBase)
    end.

random_period(Base) ->
    max(300, ceil(Base * rand:uniform())).

random_element(List) ->
    lists:nth(rand:uniform(length(List)), List).
