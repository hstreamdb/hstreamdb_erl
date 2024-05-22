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

-module(hstreamdb_reader).

-include("hstreamdb.hrl").

-behaviour(gen_server).

-export([
    read_key_shard/3,
    read_key_shard/4,
    read_key/3,
    read_key/4
]).

-export([
    connect/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([options/0]).

-type options() :: #{
    mgr_client_options := hsteamdb_client:options(),
    stream := hstreamdb:stream(),

    pool_size => non_neg_integer(),
    auto_reconnect => false | pos_integer(),

    reader_client_option_overrides => hstreamdb_client:rpc_options(),

    key_manager_options => hstreamdb_auto_key_mgr:options(),
    shard_client_manager_options => hstreamdb_shard_client_mgr:options()
}.

%% @doc Identify the shard that a key belongs to and fold all read records.
%% by default, the fold function will filter all records that have
%% exactly the same key as the one provided.

-spec read_key_shard(ecpool:pool_name(), hstreamdb:partitioning_key(), hstreamdb:limits()) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_key_shard(Reader, Key, Limits) ->
    read_key_shard(Reader, Key, Limits, {fold_stream_key_fun(Key), []}).

-spec read_key_shard(ecpool:pool_name(), hstreamdb:partitioning_key(), hstreamdb:limits(), {
    hsteamdb:reader_fold_fun(), hsteamdb:reader_fold_acc()
}) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_key_shard(Reader, Key, Limits, {FoldFun, InitAcc}) ->
    case ecpool_request(Reader, {get_shard_gstream, Key, Limits}) of
        {ok, GStream} ->
            hstreamdb_client:fold_shard_read_gstream(GStream, FoldFun, InitAcc);
        {error, _} = Error ->
            Error
    end.

-spec read_key(ecpool:pool_name(), hstreamdb:partitioning_key(), hstreamdb:limits()) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_key(Reader, Key, Limits) ->
    read_key(Reader, Key, Limits, {fold_stream_key_fun(Key), []}).

-spec read_key(ecpool:pool_name(), hstreamdb:partitioning_key(), hstreamdb:limits(), {
    hsteamdb:reader_fold_fun(), hsteamdb:reader_fold_acc()
}) ->
    {ok, [hstreamdb:hrecord()]} | {error, term()}.
read_key(Reader, Key, Limits, Fold) ->
    case check_limits(Reader, Key, Limits) of
        ok ->
            do_read_key(Reader, Key, Limits, Fold);
        {error, _} = Error ->
            Error
    end.

%%-------------------------------------------------------------------------------------------------
%% ecpool part

connect(Options) ->
    #{} = ReaderOptions = proplists:get_value(reader_options, Options),
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    gen_server:start_link(?MODULE, [ReaderOptions, WorkerId], []).

%% -------------------------------------------------------------------------------------------------
%% gen_server part

init([
    #{mgr_client_options := MgrClientOptions, stream := Stream, name := Reader} = Options, WorkerId
]) ->
    case hstreamdb_client:start({Reader, WorkerId}, MgrClientOptions) of
        {ok, MgrClient} ->
            KeyManagerOptions = maps:get(key_manager_options, Options, #{}),
            KeyManager = hstreamdb:start_key_manager(MgrClient, Stream, KeyManagerOptions),
            ReaderClientOptionOverrides = maps:get(reader_client_option_overrides, Options, #{}),
            ShardClientManagerOptions = maps:get(shard_client_manager_options, Options, #{
                client_override_opts => ReaderClientOptionOverrides
            }),
            ShardClientManager = hstreamdb:start_client_manager(
                MgrClient, ShardClientManagerOptions
            ),
            {ok, #{
                key_manager => KeyManager,
                shard_client_manager => ShardClientManager,
                client => MgrClient,
                stream => Stream
            }};
        {error, _} = Error ->
            Error
    end.

handle_call({get_shard_gstream, Key, Limits}, _From, State) ->
    case do_get_shard_gstream(State, Key, Limits) of
        {ok, GStream, NewState} ->
            {reply, {ok, GStream}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call({get_key_gstream, Key, Addr}, _From, State) ->
    case do_get_key_gstream(State, Key, Addr) of
        {ok, Stream, GStream, NewState} ->
            {reply, {ok, Stream, GStream}, NewState};
        {error, Reason, NewState} ->
            {reply, {error, Reason}, NewState}
    end;
handle_call({get_key_shard, Key}, _From, #{key_manager := KeyManager} = State) ->
    case hstreamdb_auto_key_mgr:choose_shard(KeyManager, Key) of
        {ok, ShardId, NewKeyManager} ->
            {reply, {ok, ShardId}, State#{key_manager => NewKeyManager}};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;
handle_call(get_lookup_client, _From, #{client := Client} = State) ->
    {reply, Client, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_call, Request}}, State}.

handle_info(_Request, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

terminate(_Reason, #{
    key_manager := KeyManager, shard_client_manager := ShardClientManager, client := Client
}) ->
    ok = hstreamdb:stop_key_manager(KeyManager),
    ok = hstreamdb:stop_client_manager(ShardClientManager),
    ok = hstreamdb_client:stop(Client),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%-------------------------------------------------------------------------------------------------
%% internal functions
%%-------------------------------------------------------------------------------------------------

do_get_shard_gstream(
    #{key_manager := KeyManager, shard_client_manager := ShardClientManager} = State, Key, Limits
) ->
    case hstreamdb_auto_key_mgr:choose_shard(KeyManager, Key) of
        {ok, ShardId, NewKeyManager} ->
            case hstreamdb_shard_client_mgr:lookup_shard_client(ShardClientManager, ShardId) of
                {ok, ShardClient, NewClientManager} ->
                    case hstreamdb_client:read_shard_gstream(ShardClient, ShardId, Limits) of
                        {ok, GStream} ->
                            {ok, GStream, State#{
                                key_manager => NewKeyManager,
                                shard_client_manager => NewClientManager
                            }};
                        {error, Reason} ->
                            {error, Reason, State#{
                                key_manager => NewKeyManager,
                                shard_client_manager => NewClientManager
                            }}
                    end;
                {error, _} = Error ->
                    {error, Error, State#{key_manager => NewKeyManager}}
            end;
        {error, Reason} ->
            {error, Reason, State}
    end.

do_read_key(Reader, Key, Limits, {FoldFun, InitAcc}) ->
    LookupClient = ecpool_request(Reader, get_lookup_client),
    case ?MEASURE({lookup_key, self(), Key}, hstreamdb_client:lookup_key(LookupClient, Key)) of
        {ok, {_Host, _Port} = Addr} ->
            case ecpool_request(Reader, {get_key_gstream, Key, Addr}) of
                {ok, Stream, GStream} ->
                    ?MEASURE(
                        {fold_key_read_gstream, Stream, Key},
                        hstreamdb_client:fold_key_read_gstream(
                            GStream, Stream, Key, Limits, FoldFun, InitAcc
                        )
                    );
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

check_limits(Reader, Key, Limits) ->
    case ecpool_request(Reader, {get_key_shard, Key}) of
        {ok, ShardId} ->
            check_key_limits(ShardId, Limits);
        {error, _} = Error ->
            Error
    end.

do_get_key_gstream(
    #{shard_client_manager := ShardClientManager, stream := Stream} = State,
    _Key,
    Addr
) ->
    case
        ?MEASURE(
            {lookup_addr_client, self(), Addr},
            hstreamdb_shard_client_mgr:lookup_addr_client(ShardClientManager, Addr)
        )
    of
        {ok, AddrClient, NewClientManager} ->
            case
                ?MEASURE(
                    {read_key_gstream, self(), Addr},
                    hstreamdb_client:read_key_gstream(AddrClient)
                )
            of
                {ok, GStream} ->
                    {ok, Stream, GStream, State#{
                        shard_client_manager => NewClientManager
                    }};
                {error, Reason} ->
                    {error, Reason, State#{
                        shard_client_manager => NewClientManager
                    }}
            end;
        {error, _} = Error ->
            {error, Error, State#{stream_addr => Addr}}
    end.

fold_stream_key_fun(Key) ->
    BinKey = iolist_to_binary(Key),
    fun
        (#{header := #{key := PK}} = Record, Acc) when BinKey =:= PK -> [Record | Acc];
        (#{header := #{key := _OtherPK}}, Acc) -> Acc;
        (eos, Acc) -> lists:reverse(Acc)
    end.

check_key_limits(ShardId, Limits) ->
    check_key_limits([from, until], ShardId, Limits).

check_key_limits([], _ShardId, _Limits) ->
    ok;
check_key_limits([Key | Rest], ActualShardId, Limits) ->
    case Limits of
        #{Key := #{offset := {recordOffset, #{shardId := ShardId}}}} ->
            case ActualShardId =:= ShardId of
                true -> check_key_limits(Rest, ActualShardId, Limits);
                false -> {error, {shard_mismatch, ActualShardId, ShardId}}
            end;
        _ ->
            check_key_limits(Rest, ActualShardId, Limits)
    end.

ecpool_request(Reader, Request) ->
    ecpool:with_client(
        Reader,
        fun(Pid) -> gen_server:call(Pid, Request) end
    ).
