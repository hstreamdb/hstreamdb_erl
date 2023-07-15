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

-export([
    start/1,
    stop/1,
    lookup_client/2,
    bad_client/2
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start(Client) ->
    #{
        client => Client,
        clients_by_shard => #{}
    }.

stop(#{clients_by_shard := Clients}) ->
    lists:foreach(
        fun hstreamdb_client:stop/1,
        maps:values(Clients)
    ).

lookup_client(
    ShardClientMgr = #{
        clients_by_shard := Clients,
        client := Client
    },
    ShardId
) ->
    case Clients of
        #{ShardId := ShardClient} ->
            {ok, ShardClient, ShardClientMgr};
        _ ->
            case hstreamdb_client:lookup_shard(Client, ShardId) of
                {ok, {Host, Port}} ->
                    %% Producer need only one channel. Because it is a sync call.
                    case hstreamdb_client:connect(Client, Host, Port, #{pool_size => 1}) of
                        {ok, NewClient} ->
                            ping_new_client(NewClient, ShardId, ShardClientMgr);
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

bad_client(ShardClientMgr = #{clients_by_shard := Clients}, ShardId) ->
    case Clients of
        #{ShardId := Client} ->
            ok = hstreamdb_client:stop(Client),
            ShardClientMgr#{
                clients_by_shard => maps:remove(ShardId, Clients)
            };
        _ ->
            ShardClientMgr
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

ping_new_client(Client, ShardId, ShardClientMgr = #{clients_by_shard := Clients}) ->
    case hstreamdb_client:echo(Client) of
        ok ->
            {ok, Client, ShardClientMgr#{
                clients_by_shard => Clients#{ShardId => Client}
            }};
        {error, Reason} ->
            ok = hstreamdb_client:stop(Client),
            logger:info("Echo failed for new client=~p: ~p~n", [
                Client, Reason
            ]),
            {error, Reason}
    end.
