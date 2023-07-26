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

-module(hstreamdb_client).

-define(GRPC_TIMEOUT, 5000).

-include("hstreamdb.hrl").

-export([
    start/1,
    start/2,
    stop/1,

    echo/1,
    create_stream/5,
    delete_stream/2,
    delete_stream/4,

    lookup_shard/2,
    list_shards/2,

    append/2,
    append/3,

    connect/3,
    connect/4,

    lookup_resource/3,

    read_single_shard_stream/3,

    read_shard_stream/3,
    fold_shard_read_gstream/3,

    name/1
]).

-export_type([
    t/0,
    name/0,
    options/0
]).

-opaque t() :: #{}.

-type name() :: atom() | string() | binary().

-type options() :: #{
    url := binary() | string(),
    rpc_options := grpc_client_sup:options(),
    host_mapping => #{binary() => binary()},
    grpc_timeout => pos_integer(),
    reap_channel => boolean()
}.

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

-spec start(options()) -> {ok, t()} | {error, term()}.
start(Options) ->
    start(random_name(), Options).

-spec start(name(), options()) -> {ok, t()} | {error, term()}.
start(Name, Options) ->
    ServerURL = maps:get(url, Options),
    RPCOptions = maps:get(rpc_options, Options),
    HostMapping = maps:get(host_mapping, Options, #{}),
    ChannelName = to_channel_name(Name),
    GRPCTimeout = maps:get(grpc_timeout, Options, ?GRPC_TIMEOUT),
    ReapChannel = maps:get(reap_channel, Options, true),
    case validate_url_and_opts(ServerURL, maps:get(gun_opts, RPCOptions, #{})) of
        {ok, ServerURLMap, GunOpts} ->
            Client =
                #{
                    channel => ChannelName,
                    url_map => ServerURLMap,
                    rpc_options => RPCOptions#{gun_opts => GunOpts},
                    host_mapping => HostMapping,
                    grpc_timeout => GRPCTimeout,
                    reap_channel => ReapChannel
                },
            case start_channel(ChannelName, ServerURLMap, RPCOptions, ReapChannel) of
                ok ->
                    {ok, Client};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

-spec connect(t(), inet:hostname() | inet:ip_address(), inet:port_number()) ->
    {ok, t()} | {error, term()}.
connect(Client, Host, Port) ->
    connect(Client, Host, Port, #{}).

-spec connect(
    t(), inet:hostname() | inet:ip_address(), inet:port_number(), grpc_client_sup:options()
) -> {ok, t()} | {error, term()}.
connect(
    #{url_map := ServerURLMap, rpc_options := RPCOptions, reap_channel := ReapChannel} = Client,
    Host0,
    Port,
    RPCOptionsOverrides
) ->
    Host1 = map_host(Client, Host0),
    NewChannelName = new_channel_name(Client, Host1, Port),
    NewUrlMap = maps:merge(ServerURLMap, #{
        host => Host1,
        port => Port
    }),
    NewRPCOptions = maps:merge(RPCOptions, RPCOptionsOverrides),
    case start_channel(NewChannelName, NewUrlMap, NewRPCOptions, ReapChannel) of
        ok ->
            {ok, Client#{
                channel := NewChannelName, url_map := NewUrlMap, rpc_options := NewRPCOptions
            }};
        {error, Reason} ->
            {error, Reason}
    end.

-spec stop(t()) -> ok.
stop(#{channel := Channel}) ->
    grpc_client_sup:stop_channel_pool(Channel),
    ok = unregister_channel(Channel);
stop(Name) ->
    Channel = to_channel_name(Name),
    grpc_client_sup:stop_channel_pool(Channel),
    ok = unregister_channel(Channel).

-spec name(t()) -> name().
name(#{channel := Channel}) ->
    Channel.

echo(Client) ->
    do_echo(Client).

create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount) ->
    do_create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount).

delete_stream(Client, Name) ->
    delete_stream(Client, Name, true, true).

delete_stream(Client, Name, IgnoreNonExist, Force) ->
    do_delete_stream(Client, Name, IgnoreNonExist, Force).

lookup_shard(Client, ShardId) ->
    do_lookup_shard(Client, ShardId).

list_shards(Client, StreamName) ->
    do_list_shards(Client, StreamName).

append(Client, Req) ->
    append(Client, Req, #{}).

append(Client, Req, Options) ->
    do_append(Client, Req, Options).

lookup_resource(Client, ResourceType, ResourceId) ->
    do_lookup_resource(Client, ResourceType, ResourceId).

read_single_shard_stream(Client, StreamName, Limits) ->
    do_read_single_shard_stream(Client, StreamName, Limits).

read_shard_stream(Client, ShardId, Limits) ->
    do_read_shard_stream(Client, ShardId, Limits).

fold_shard_read_gstream(GStream, Fun, Acc) ->
    do_fold_shard_read_gstream(GStream, Fun, Acc).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Channel creation

start_channel(ChannelName, URLMap, RPCOptions, ReapChannel) ->
    URL = uri_string:recompose(URLMap),
    case grpc_client_sup:create_channel_pool(ChannelName, URL, RPCOptions) of
        {ok, _, _} ->
            ok = register_channel(ChannelName, ReapChannel);
        {ok, _} ->
            ok = register_channel(ChannelName, ReapChannel);
        {error, Reason} ->
            {error, Reason}
    end.

%% GRPC methods

do_echo(#{channel := Channel, grpc_timeout := Timeout}) ->
    case ?HSTREAMDB_GEN_CLIENT:echo(#{}, #{channel => Channel, timeout => Timeout}) of
        {ok, #{msg := _}, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_create_stream(
    #{channel := Channel, grpc_timeout := Timeout},
    Name,
    ReplFactor,
    BacklogDuration,
    ShardCount
) ->
    Req = #{
        streamName => Name,
        replicationFactor => ReplFactor,
        backlogDuration => BacklogDuration,
        shardCount => ShardCount
    },
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:create_stream(Req, Options) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_delete_stream(
    #{channel := Channel, grpc_timeout := Timeout},
    Name,
    IgnoreNonExist,
    Force
) ->
    Req = #{
        streamName => Name,
        ignoreNonExist => IgnoreNonExist,
        force => Force
    },
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:delete_stream(Req, Options) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_lookup_shard(#{channel := Channel, grpc_timeout := Timeout}, ShardId) ->
    Req = #{shardId => ShardId},
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:lookup_shard(Req, Options) of
        {ok, #{serverNode := #{host := Host, port := Port}}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

do_list_shards(#{channel := Channel, grpc_timeout := Timeout}, StreamName) ->
    Req = #{streamName => StreamName},
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:list_shards(Req, Options) of
        {ok, #{shards := Shards}, _} ->
            logger:info("[hstreamdb] fetched shards for stream ~p: ~p~n", [StreamName, Shards]),
            {ok, Shards};
        {error, _} = Error ->
            Error
    end.

do_append(
    #{channel := Channel, grpc_timeout := Timeout},
    #{
        stream_name := StreamName,
        records := Records,
        shard_id := ShardId,
        compression_type := CompressionType
    },
    OptionOverrides
) ->
    case encode_records(Records, CompressionType) of
        {ok, NRecords} ->
            BatchedRecord = #{
                payload => NRecords,
                batchSize => length(Records),
                compressionType => compression_type_to_enum(CompressionType)
            },
            NReq = #{streamName => StreamName, shardId => ShardId, records => BatchedRecord},
            Options = maps:merge(#{channel => Channel, timeout => Timeout}, OptionOverrides),
            case timer:tc(fun() -> ?HSTREAMDB_GEN_CLIENT:append(NReq, Options) end) of
                {Time, {ok, Resp, _MetaData}} ->
                    logger:info(
                        "[hstreamdb] flush_request[~p, ~p], pid=~p, SUCCESS, ~p records in ~p ms~n",
                        [
                            Channel, ShardId, self(), length(Records), Time div 1000
                        ]
                    ),
                    {ok, Resp};
                {Time, {error, R}} ->
                    logger:error(
                        "flush_request[~p, ~p], pid=~p, timeout=~p, ERROR: ~p, in ~p ms~n", [
                            Channel, ShardId, self(), Timeout, R, Time div 1000
                        ]
                    ),
                    {error, R}
            end;
        {error, R} ->
            {error, R}
    end.

do_lookup_resource(
    #{channel := Channel, grpc_timeout := Timeout},
    ResourceType,
    ResourceId
) ->
    Req = #{resType => ResourceType, resId => ResourceId},
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:lookup_resource(Req, Options) of
        {ok, #{host := Host, port := Port}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

do_read_single_shard_stream(
    #{channel := Channel, grpc_timeout := Timeout},
    StreamName,
    Opts
) ->
    Req0 = #{
        streamName => StreamName,
        readerId => integer_to_binary(erlang:unique_integer([positive]))
    },
    Limits = maps:get(limits, Opts, #{}),
    Req = maps:merge(Req0, map_keys([{max_read_batches, maxReadBatches}], Limits)),
    Options = #{channel => Channel, timeout => Timeout},
    logger:debug("[hstreamdb] read_single_shard: Req: ~p~nOptions: ~p~n", [Req, Options]),
    case ?HSTREAMDB_GEN_CLIENT:read_single_shard_stream(Options) of
        {ok, GStream} ->
            ok = grpc_client:send(GStream, Req, fin),
            {FoldFun, Acc} = maps:get(fold, Opts, {fun append_rec/2, []}),
            do_fold_shard_read_gstream(GStream, FoldFun, Acc);
        {error, _} = Error ->
            Error
    end.

do_read_shard_stream(
    #{channel := Channel, grpc_timeout := Timeout},
    ShardId,
    Limits
) ->
    Req0 = #{
        shardId => ShardId,
        readerId => integer_to_binary(erlang:unique_integer([positive]))
    },
    Req = maps:merge(Req0, map_keys([{max_read_batches, maxReadBatches}], Limits)),
    Options = #{channel => Channel, timeout => Timeout},
    case ?HSTREAMDB_GEN_CLIENT:read_shard_stream(Options) of
        {ok, GStream} ->
            ok = grpc_client:send(GStream, Req, fin),
            {ok, GStream};
        {error, _} = Error ->
            Error
    end.

%% Helper functions

append_rec(eos, Acc) -> lists:reverse(Acc);
append_rec(Rec, Acc) -> {ok, [Rec | Acc]}.

map_keys([], Map) ->
    Map;
map_keys([{KeyOld, KeyNew} | Rest], Map) ->
    case Map of
        #{KeyOld := Value} ->
            map_keys(Rest, maps:put(KeyNew, Value, maps:remove(KeyOld, Map)));
        _ ->
            map_keys(Rest, Map)
    end.

validate_url_and_opts(URL, GunOpts) when is_binary(URL) ->
    validate_url_and_opts(list_to_binary(URL), GunOpts);
validate_url_and_opts(URL, GunOpts) when is_list(URL) ->
    case uri_string:parse(URL) of
        {error, What, Term} ->
            {error, {invalid_url, What, Term}};
        URIMap when is_map(URIMap) ->
            validate_scheme_and_opts(set_default_port(URIMap), GunOpts)
    end.

set_default_port(#{port := _Port} = URIMap) ->
    URIMap;
set_default_port(URIMap) ->
    URIMap#{port => ?DEFAULT_HSTREAMDB_PORT}.

validate_scheme_and_opts(#{scheme := "hstreams"} = URIMap, GunOpts) ->
    validate_scheme_and_opts(URIMap#{scheme := "https"}, GunOpts);
validate_scheme_and_opts(#{scheme := "hstream"} = URIMap, GunOpts) ->
    validate_scheme_and_opts(URIMap#{scheme := "http"}, GunOpts);
validate_scheme_and_opts(#{scheme := "https"}, #{transport := tcp}) ->
    {error, {https_invalid_transport, tcp}};
validate_scheme_and_opts(#{scheme := "https"} = URIMap, GunOpts) ->
    {ok, URIMap, GunOpts#{transport => tls}};
validate_scheme_and_opts(#{scheme := "http"}, #{transport := tls}) ->
    {error, {http_invalid_transport, tls}};
validate_scheme_and_opts(#{scheme := "http"} = URIMap, GunOpts) ->
    {ok, URIMap, GunOpts#{transport => tcp}};
validate_scheme_and_opts(_URIMap, _GunOpts) ->
    {error, unknown_scheme}.

map_host(#{host_mapping := HostMapping} = _Client, Host) ->
    host_to_string(maps:get(Host, HostMapping, Host)).

new_channel_name(#{channel := ChannelName}, Host, Port) ->
    lists:concat([
        ChannelName,
        "_",
        to_channel_name(Host),
        "_",
        to_channel_name(Port),
        "_",
        to_channel_name(erlang:unique_integer([positive]))
    ]).

register_channel(ChannelName, true) ->
    hstreamdb_channel_reaper:register_channel(ChannelName);
register_channel(_ChannelName, false) ->
    ok.

unregister_channel(ChannelName) ->
    hstreamdb_channel_reaper:unregister_channel(ChannelName).

host_to_string(Host) when is_list(Host) ->
    Host;
host_to_string(Host) when is_binary(Host) ->
    unicode:characters_to_list(Host).

to_channel_name(Integer) when is_integer(Integer) ->
    integer_to_list(Integer);
to_channel_name(Atom) when is_atom(Atom) ->
    atom_to_list(Atom);
to_channel_name(Binary) when is_binary(Binary) ->
    unicode:characters_to_list(Binary);
to_channel_name(List) when is_list(List) ->
    List.

encode_records(Records, CompressionType) ->
    case safe_encode_msg(Records) of
        {ok, Payload} ->
            case CompressionType of
                none -> {ok, Payload};
                gzip -> gzip(Payload);
                zstd -> zstd(Payload)
            end;
        {error, _} = Error ->
            Error
    end.

compression_type_to_enum(CompressionType) ->
    case CompressionType of
        none -> 0;
        gzip -> 1;
        zstd -> 2
    end.

safe_encode_msg(Records) ->
    try
        Payload = hstreamdb_api:encode_msg(
            #{records => Records},
            batch_h_stream_records,
            []
        ),
        {ok, Payload}
    catch
        error:Reason -> {error, {encode_msg, Reason}}
    end.

gzip(Payload) ->
    try
        {ok, zlib:gzip(Payload)}
    catch
        error:Reason -> {error, {gzip, Reason}}
    end.

zstd(Payload) ->
    case ezstd:compress(Payload) of
        {error, R} -> {error, {zstd, R}};
        R -> {ok, R}
    end.

do_fold_shard_read_gstream(GStream, Fun, Acc) ->
    case grpc_client:recv(GStream) of
        {ok, Results} ->
            logger:debug("[hstreamdb] Ok recv~n"),
            case fold_results(Results, Fun, Acc) of
                {ok, NewAcc} ->
                    fold_shard_read_gstream(GStream, Fun, NewAcc);
                {stop, NewAcc} ->
                    {ok, NewAcc};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            logger:debug("[hstreamdb] Error recv~n"),
            Error
    end.

fold_results([], _Fun, Acc) ->
    {ok, Acc};
fold_results([Result | Rest], Fun, Acc) ->
    case fold_result(Result, Fun, Acc) of
        {ok, NewAcc} ->
            fold_results(Rest, Fun, NewAcc);
        {stop, NewAcc} ->
            {stop, NewAcc};
        {error, _} = Error ->
            Error
    end.

fold_result(#{receivedRecords := Records}, Fun, Acc) ->
    fold_batch_records(Records, Fun, Acc);
fold_result({eos, _}, Fun, Acc) ->
    {stop, Fun(eos, Acc)}.

fold_batch_records([], _Fun, Acc) ->
    {ok, Acc};
fold_batch_records([Record | Rest], Fun, Acc) ->
    case fold_batch_record(Record, Fun, Acc) of
        {ok, NewAcc} ->
            fold_batch_records(Rest, Fun, NewAcc);
        {stop, NewAcc} ->
            {stop, NewAcc}
    end.

fold_batch_record(#{record := _, recordIds := [#{batchId := BatchId} | _]} = BatchRecord, Fun, Acc) ->
    Records = decode_batch(BatchRecord),
    logger:debug("[hstreamdb] BatchRecord, id: ~p, records: ~p~n", [BatchId, length(Records)]),
    fold_hstream_records(Records, Fun, Acc).

fold_hstream_records([Record | Records], Fun, Acc) ->
    case Fun(Record, Acc) of
        {ok, NewAcc} ->
            fold_hstream_records(Records, Fun, NewAcc);
        {stop, NewAcc} ->
            {stop, NewAcc}
    end;
fold_hstream_records([], _Fun, Acc) ->
    {ok, Acc}.

decode_batch(#{
    record := #{payload := Payload0, compressionType := CompressionType} = _BatchRecord,
    recordIds := RecordIds
}) ->
    Payload1 = decode_payload(Payload0, CompressionType),
    #{records := Records} = hstreamdb_api:decode_msg(Payload1, batch_h_stream_records, []),
    lists:zipwith(
        fun(RecordId, Record) ->
            Record#{recordId => RecordId}
        end,
        RecordIds,
        Records
    ).

decode_payload(Payload, 'None') ->
    Payload;
decode_payload(Payload, 'Gzip') ->
    zlib:gunzip(Payload);
decode_payload(Payload, 'Zstd') ->
    case ezstd:decompress(Payload) of
        {error, Error} -> error({zstd, Error});
        Bin when is_binary(Bin) -> Bin
    end.

random_name() ->
    "hstreandb-client-" ++ integer_to_list(erlang:system_time()) ++ "-" ++
        integer_to_list(erlang:unique_integer([positive])).
