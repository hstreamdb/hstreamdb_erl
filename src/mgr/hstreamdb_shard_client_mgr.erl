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
-module(hstreamdb_shard_client_mgr).

-include("hstreamdb.hrl").

-export([
    start/1,
    start/2,
    stop/1,
    lookup_shard_client/2,
    lookup_addr_client/2,
    bad_shard_client/2,
    client/1
]).

-export_type([t/0, options/0]).

-define(DEFAULT_OPTS, #{
    cache_by_shard_id => false,
    client_override_opts => #{}
}).

-type options() :: #{
    cache_by_shard_id => boolean(),
    client_override_opts => grpc_client_sup:options()
}.

-opaque t() :: #{
    client_by_addr := #{},
    addr_by_shard := #{},
    client_override_opts := grpc_client_sup:options(),
    cache_by_shard_id := boolean(),
    client := hstreamdb:client()
}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start(hstreamdb:client()) -> t().
start(Client) ->
    start(Client, ?DEFAULT_OPTS).

-spec start(hstreamdb:client(), options()) -> t().
start(Client, Opts0) ->
    Opts1 = maps:merge(?DEFAULT_OPTS, maps:with(maps:keys(?DEFAULT_OPTS), Opts0)),
    maps:merge(
        #{
            client => Client,
            addr_by_shard => #{},
            client_by_addr => #{}
        },
        Opts1
    ).

-spec stop(t()) -> ok.
stop(#{client_by_addr := Clients}) ->
    lists:foreach(
        fun hstreamdb_client:stop/1,
        maps:values(Clients)
    ).

-spec lookup_shard_client(t(), hstreamdb:shard_id()) -> {ok, hstreamdb:client(), t()} | {error, term()}.
lookup_shard_client(
    ShardClientMgr0 = #{
        addr_by_shard := Addrs,
        client := Client
    },
    ShardId
) ->
    case Addrs of
        #{ShardId := Addr} ->
            client_by_addr(ShardClientMgr0, Addr);
        _ ->
            case hstreamdb_client:lookup_resource(Client, ?RES_SHARD, integer_to_binary(ShardId)) of
                {ok, {_Host, _Port} = Addr} ->
                    ShardClientMgr1 = cache_shard_addr(ShardClientMgr0, ShardId, Addr),
                    client_by_addr(ShardClientMgr1, Addr);
                {error, _} = Error ->
                    Error
            end
    end.

-spec lookup_addr_client(t(), hstreamdb:stream()) -> {ok, hstreamdb:client(), t()} | {error, term()}.
lookup_addr_client(ShardClientMgr, Addr) ->
    client_by_addr(ShardClientMgr, Addr).

-spec bad_shard_client(t(), hstreamdb:client()) -> t().
bad_shard_client(ShardClientMgr = #{client_by_addr := Clients}, ShardClient) ->
    NewClents = maps:filter(
        fun(_Addr, Client) ->
            hstreamdb_client:name(Client) =/= hstreamdb_client:name(ShardClient)
        end,
        Clients
    ),
    ok = hstreamdb_client:stop(ShardClient),
    ShardClientMgr#{
        client_by_addr := NewClents
    }.

-spec client(t()) -> hstreamdb:client().
client(#{client := Client}) ->
    Client.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

cache_shard_addr(#{cache_by_shard_id := false} = ShardClientMgr, _ShardId, _Addr) ->
    ShardClientMgr;
cache_shard_addr(#{addr_by_shard := Addrs} = ShardClientMgr, ShardId, Addr) ->
    ShardClientMgr#{
        addr_by_shard => maps:put(ShardId, Addr, Addrs)
    }.

client_by_addr(
    #{
        client_by_addr := Clients,
        client_override_opts := OverrideOpts,
        client := Client
    } = ShardClientMgr,
    Addr
) ->
    case Clients of
        #{Addr := AddrClient} ->
            {ok, AddrClient, ShardClientMgr};
        _ ->
            {Host, Port} = Addr,
            case hstreamdb_client:connect(Client, Host, Port, OverrideOpts) of
                {ok, NewClient} ->
                    ping_new_client(NewClient, Addr, ShardClientMgr);
                {error, _} = Error ->
                    Error
            end
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ping_new_client(Client, Addr, ShardClientMgr = #{client_by_addr := Clients}) ->
    case hstreamdb_client:echo(Client) of
        ok ->
            {ok, Client, ShardClientMgr#{
                client_by_addr := Clients#{Addr => Client}
            }};
        {error, Reason} ->
            ok = hstreamdb_client:stop(Client),
            logger:info("[hstreamdb] Echo failed for new client=~p: ~p~n", [
                Client, Reason
            ]),
            {error, Reason}
    end.
