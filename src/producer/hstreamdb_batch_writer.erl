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

-module(hstreamdb_batch_writer).

-behaviour(gen_server).

-include("errors.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    start_link/1,
    stop/1,
    write/2,
    connect/1
]).

-export([
    on_shards_updated/3,
    on_init/1,
    on_terminate/4
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

-define(DEFAULT_GRPC_TIMEOUT, 30000).

-include("hstreamdb.hrl").

-record(state, {
    name,
    stream,
    grpc_timeout
}).

start_link(Opts) ->
    gen_server:start_link(?MODULE, [Opts], []).

write(Pid, Batch) ->
    gen_server:cast(Pid, {write, Batch, self()}).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%--------------------------------------------------------------------------------------------------
%% ecpool part
%%--------------------------------------------------------------------------------------------------

-type options() :: #{
    name := ecpool:pool_name(),
    stream := hstreamdb:stream(),
    grpc_timeout => non_neg_integer()
}.

-spec start_link(options()) -> gen_server:start_ret().
connect(PoolOptions) ->
    Options = proplists:get_value(opts, PoolOptions),
    start_link(Options).

%%-------------------------------------------------------------------------------------------------
%% Discovery
%%-------------------------------------------------------------------------------------------------

-define(SHARD_CLIENT_KEY(NAME, SHARD_ID), {?MODULE, NAME, SHARD_ID}).

on_init(Name) -> cleanup(Name).
on_terminate(Name, _Vsn, _KeyManager, _ShardClientMgr) -> cleanup(Name).

on_shards_updated(Name, {OldVsn, _OldKeyMgr, _OldClientMgr}, {NewVsn, NewKeyMgr, NewClientMgr}) ->
    ?tp(hstreamdb_batch_writer_on_shards_updated, #{
        name => Name, old_vsn => OldVsn, new_vsn => NewVsn
    }),
    {ok, ShardIds} = hstreamdb_key_mgr:shard_ids(NewKeyMgr),
    ok = lists:foreach(
        fun(ShardId) ->
            {ok, ShardClient, _ClientMgr} = hstreamdb_shard_client_mgr:lookup_shard_client(
                NewClientMgr, ShardId
            ),
            ok = set_shard_client(Name, ShardId, NewVsn, ShardClient)
        end,
        ShardIds
    ),
    true = ets:match_delete(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(Name, '_'), {OldVsn, '_'}}),
    ok.

cleanup(Name) ->
    true = ets:match_delete(?DISCOVERY_TAB, {?SHARD_CLIENT_KEY(Name, '_'), '_'}),
    ok.

shard_client(Name, ShardId) ->
    case ets:lookup(?DISCOVERY_TAB, ?SHARD_CLIENT_KEY(Name, ShardId)) of
        [] ->
            not_found;
        [{_, {Version, ShardClient}}] ->
            {ok, Version, ShardClient}
    end.

set_shard_client(Name, ShardId, Version, ShardClient) ->
    true = ets:insert(?DISCOVERY_TAB, {
        ?SHARD_CLIENT_KEY(Name, ShardId), {Version, ShardClient}
    }),
    ok.

%%-------------------------------------------------------------------------------------------------
%% gen_server
%%-------------------------------------------------------------------------------------------------

init([Opts]) ->
    process_flag(trap_exit, true),

    StreamName = maps:get(stream, Opts),
    Name = maps:get(name, Opts),
    GRPCTimeout = maps:get(grpc_timeout, Opts, ?DEFAULT_GRPC_TIMEOUT),
    {ok, #state{
        stream = StreamName,
        name = Name,
        grpc_timeout = GRPCTimeout
    }}.

handle_cast(
    {write, #batch{shard_id = ShardId, batch_ref = BatchRef, req_ref = ReqRef} = Batch, Caller},
    State
) ->
    ?LOG_DEBUG("[hstreamdb] producer_batch_writer, received, batch ref: ~p, req ref: ~p", [
        BatchRef, ReqRef
    ]),
    Records = records(Batch),
    {Result, NState} = do_write(ShardId, Records, Batch, State),
    ?LOG_DEBUG(
        "[hstreamdb] producer_batch_writer, handled, batch ref: ~p, req ref: ~p, result: ~p", [
            BatchRef, ReqRef, Result
        ]
    ),
    ok = hstreamdb_batch_aggregator:report_result(Caller, Batch, Result),
    {noreply, NState}.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #state{name = Name} = _State) ->
    ?LOG_DEBUG("[hstreamdb] batch_writer ~p terminating", [Name]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% -------------------------------------------------------------------------------------------------
%% internal functions

records(#batch{batch_ref = BatchId, tab = Tab}) ->
    case ets:lookup(Tab, BatchId) of
        [{_, Records}] ->
            prepare_known_resps(Records);
        [] ->
            {[], []}
    end.

prepare_known_resps(Records) ->
    Now = erlang:monotonic_time(millisecond),
    {Resps, Reqs} = lists:unzip(
        lists:map(
            fun(BufferRecord) ->
                Deadline = hstreamdb_buffer:deadline(BufferRecord),
                case Deadline of
                    infinity ->
                        {undefined, hstreamdb_buffer:data(BufferRecord)};
                    T when T >= Now ->
                        {undefined, hstreamdb_buffer:data(BufferRecord)};
                    _ ->
                        {{error, ?ERROR_RECORD_TIMEOUT}, undefined}
                end
            end,
            Records
        )
    ),
    {Resps, lists:filter(fun(X) -> X =/= undefined end, Reqs)}.

do_write(_ShardId, {Resps, []}, #batch{batch_ref = BatchRef, req_ref = ReqRef}, State) ->
    ?LOG_DEBUG(
        "[hstreamdb] producer_batch_writer, do_write, batch ref: ~p, req ref: ~p,~n"
        "premature resps: ~p, records: 0",
        [BatchRef, ReqRef, length(Resps)]
    ),
    {{ok, Resps}, State};
do_write(
    ShardId,
    {Resps, Records},
    #batch{compression_type = CompressionType, batch_ref = BatchRef, req_ref = ReqRef},
    State = #state{
        name = Name,
        stream = Stream,
        grpc_timeout = GRPCTimeout
    }
) ->
    ?LOG_DEBUG(
        "[hstreamdb] producer_batch_writer, do_write, batch ref: ~p, req ref: ~p,~n"
        "premature resps: ~p, records: ~p",
        [BatchRef, ReqRef, length(Resps), length(Records)]
    ),
    case shard_client(Name, ShardId) of
        {ok, Version, Client} ->
            Req = #{
                stream_name => Stream,
                records => Records,
                shard_id => ShardId,
                compression_type => CompressionType
            },
            Options = #{
                timeout => GRPCTimeout
            },
            try hstreamdb_client:append(Client, Req, Options) of
                {ok, #{recordIds := RecordsIds}} ->
                    Res = fill_responses(Resps, RecordsIds),
                    {{ok, Res}, State};
                {error, _} = Error ->
                    ?LOG_WARNING(
                        "[hstreamdb] producer_batch_writer, do_write, batch ref: ~p, req ref: ~p,~n"
                        "append failed, error: ~p",
                        [BatchRef, ReqRef, Error]
                    ),
                    hstreamdb_discovery:report_shard_unavailable(Name, Version, ShardId, Error),
                    {Error, State}
            catch
                %% Discovery dropped the clients
                error:badarg ->
                    {{error, {cannot_access_shard, badarg}}, State};
                %% Discovery dropped the clients
                exit:{noproc, _} ->
                    {{error, {cannot_access_shard, noproc}}, State};
                error:Other ->
                    {{error, Other}, State}
            end;
        not_found ->
            {{error, cannot_resolve_shard_id}, State}
    end.

fill_responses(KnownResps, Ids) ->
    fill_responses(KnownResps, Ids, []).

fill_responses([], [], Acc) ->
    lists:reverse(Acc);
fill_responses([undefined | Rest], [Id | Ids], Acc) ->
    fill_responses(Rest, Ids, [{ok, Id} | Acc]);
fill_responses([Resp | Rest], Ids, Acc) ->
    fill_responses(Rest, Ids, [Resp | Acc]).
