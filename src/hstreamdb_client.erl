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
-include_lib("kernel/include/logger.hrl").

-export([
    start/1,
    start/2,
    stop/1,

    create/1,
    create/2,

    echo/1,
    create_stream/5,
    delete_stream/2,
    delete_stream/4,

    lookup_shard/2,
    list_shards/2,

    append/2,
    append/3,

    connect/1,
    connect/3,
    connect/4,

    reconnect/1,

    lookup_resource/3,
    lookup_key/2,

    read_single_shard_stream/3,

    read_shard_gstream/3,
    fold_shard_read_gstream/3,

    read_key_gstream/1,
    fold_key_read_gstream/6,

    trim/3,
    trim/4,

    name/1,
    options/1,
    metadata/1
]).

-export_type([
    t/0,
    name/0,
    rpc_options/0,

    replication_factor/0,
    backlog_duration/0,
    shard_count/0,
    addr/0,
    stream/0,
    partitioning_key/0,
    shard_id/0,
    shard/0,

    hrecord/0,
    hrecord_req/0,

    offset/0,
    limits_shard/0,
    limits_key/0
]).

-define(READ_KEY_STEP_COUNT, 200).

-define(DEFAULT_RPC_OPTIONS, #{
    pool_size => 1
}).

-type t() :: #{
    channel := term(),
    sup_ref := gen_server:server_ref() | undefined,
    grpc_timeout := non_neg_integer(),
    url := binary() | string(),
    url_maps := [map()],
    host_mapping := map(),
    reap_channel := boolean(),
    rpc_options := map(),
    metadata := map()
}.

-type name() :: atom() | string() | binary() | tuple().

-type replication_factor() :: pos_integer().
-type backlog_duration() :: pos_integer().
-type shard_count() :: pos_integer().

-type addr() :: {binary(), inet:port_number()}.

-type stream() :: binary() | string().
-type partitioning_key() :: binary() | string().
-type shard_id() :: integer().
-type trim_point() :: binary().

-type shard() :: #{
    shardId := shard_id(),
    startHashRangeKey := binary(),
    endHashRangeKey := binary(),
    isActive := boolean()
}.

-type hrecord_attributes() :: #{}.
-type hrecord_header() :: #{
    flag := atom() | integer(),
    key := partitioning_key(),
    attributes => hrecord_attributes()
}.
-type hrecord_payload() :: binary().
-type hrecord_id() :: #{
    batchId := integer(),
    batchIndex := integer(),
    shardId := shard_id()
}.
-type hrecord() :: #{
    header := hrecord_header(),
    payload := hrecord_payload(),
    recordId := hrecord_id()
}.

-type hrecord_req() :: #{
    header := hrecord_header(),
    payload := hrecord_payload()
}.

-type special_offset() :: {specialOffset, 0 | 1}.
-type timestamp_offset() :: {timestampOffset, #{timestampInMs => integer()}}.
-type record_offset() :: {recordOffset, hrecord_id()}.

-type offset() :: #{offset => special_offset() | timestamp_offset() | record_offset()}.

-type limits_shard() :: #{from => offset(), until => offset(), maxReadBatches => non_neg_integer()}.
-type limits_key() :: #{from => offset(), until => offset(), readRecordCount => non_neg_integer()}.

-type reader_fold_acc() :: term().
-type reader_fold_fun() :: fun((hrecord() | eos, reader_fold_acc()) -> reader_fold_acc()).

-type compression_type() :: none | gzip | zstd.

-type append_req() :: #{
    stream_name := stream(),
    records => [hrecord_req()],
    shard_id := shard_id(),
    compression_type => compression_type()
}.

-type append_res() :: #{
    recordIds := [hrecord_id()]
}.

%% grpc_client:client_opts() + pool_size option
-type rpc_options() :: #{
    pool_size => pos_integer(),
    gun_opts => gun:opts(),
    stream_batch_size => non_neg_integer(),
    stream_batch_delay_ms => non_neg_integer(),
    _ => _
}.

-type options() :: #{
    url := binary() | string(),
    rpc_options => rpc_options(),
    sup_ref => gen_server:server_ref(),
    host_mapping => #{binary() => binary()},
    grpc_timeout => pos_integer(),
    reap_channel => boolean(),
    metadata => map(),
    username => binary() | string(),
    password => binary() | string()
}.

-type grpc_req_options() :: #{
    timeout => non_neg_integer(),
    _ => _
}.

%% ?RES_XXX in hstreamdb.hrl
-type resource_type() :: integer().
-type resource_id() :: binary() | string().

-type gstream() :: grpc_client:grpcstream().

%%--------------------------------------------------------------------
%% API functions
%%--------------------------------------------------------------------

-spec start(options()) -> {ok, t()} | {error, term()}.
start(Options) ->
    start(random_name(), Options).

-spec start(name(), options()) -> {ok, t()} | {error, term()}.
start(Name, Options) ->
    case create(Name, Options) of
        {ok, Client} ->
            case connect(Client) of
                ok ->
                    {ok, Client};
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

-spec create(options()) -> {ok, t()} | {error, term()}.
create(Options) ->
    create(random_name(), Options).

-spec create(name(), options()) -> {ok, t()} | {error, term()}.
create(Name, Options) ->
    ServerURL = maps:get(url, Options),
    RPCOptions = maps:get(rpc_options, Options, ?DEFAULT_RPC_OPTIONS),
    HostMapping = maps:get(host_mapping, Options, #{}),
    ChannelName = to_channel_name(Name),
    GRPCTimeout = maps:get(grpc_timeout, Options, ?GRPC_TIMEOUT),
    ReapChannel = maps:get(reap_channel, Options, false),
    SupRef = maps:get(sup_ref, Options, undefined),
    Metadata = prepare_metadata(Options),
    GunOpts0 = maps:get(gun_opts, RPCOptions, #{}),
    validate_urls_and_opts(ServerURL, GunOpts0, fun(ServerURLMaps, GunOpts1) ->
        validate_reap_sup_ref(ReapChannel, SupRef, fun() ->
            {ok, #{
                channel => ChannelName,
                url => ServerURL,
                url_maps => ServerURLMaps,
                rpc_options => RPCOptions#{gun_opts => GunOpts1},
                host_mapping => HostMapping,
                grpc_timeout => GRPCTimeout,
                reap_channel => ReapChannel,
                metadata => Metadata,
                sup_ref => SupRef
            }}
        end)
    end).

-spec reconnect(t()) -> ok | {error, term()}.
reconnect(Client) ->
    _ = stop(Client),
    connect(Client).

-spec connect(t()) -> ok | {error, term()}.
connect(#{url_maps := UrlMaps} = Client) ->
    connect(Client, shuffle(UrlMaps)).

connect(_Client, []) ->
    {error, no_more_urls_to_connect};
connect(
    #{
        channel := ChannelName,
        rpc_options := RPCOptions,
        reap_channel := ReapChannel,
        sup_ref := SupRef
    } = Client,
    [URLMap | Rest]
) ->
    try
        case start_channel(SupRef, ChannelName, URLMap, RPCOptions, ReapChannel) of
            ok ->
                case echo(Client) of
                    ok ->
                        ok;
                    {error, Reason} ->
                        ?LOG_ERROR(
                            "[hstreamdb] ping new connection failed: ~p,~nurl: ~p, channel: ~p~n",
                            [Reason, uri_string:recompose(URLMap), ChannelName]
                        ),
                        _ = stop(Client),
                        connect(Client, Rest)
                end;
            {error, Reason} ->
                ?LOG_ERROR(
                    "[hstreamdb] start new connection failed: ~p,~nurl: ~p, channel: ~p",
                    [Reason, uri_string:recompose(URLMap), ChannelName]
                ),
                _ = stop(Client),
                connect(Client, Rest)
        end
    catch
        Class:CrashReason:Stack ->
            ?LOG_ERROR(
                "[hstreamdb] start new connection crashed: ~p:~p:~p,~nurl: ~p,~nchannel: ~p",
                [Class, CrashReason, Stack, uri_string:recompose(URLMap), ChannelName]
            ),
            _ = stop(Client),
            connect(Client, Rest)
    end.

-spec connect(t(), inet:hostname() | inet:ip_address(), inet:port_number()) ->
    {ok, t()} | {error, term()}.
connect(Client, Host, Port) ->
    connect(Client, Host, Port, #{}).

-spec connect(
    t(), inet:hostname() | inet:ip_address(), inet:port_number(), rpc_options()
) -> {ok, t()} | {error, term()}.
connect(
    #{
        url_maps := [ServerURLMap | _],
        rpc_options := RPCOptions,
        reap_channel := ReapChannel,
        grpc_timeout := GRPCTimeout,
        sup_ref := SupRef
    } = Client,
    Host0,
    Port0,
    RPCOptionsOverrides
) ->
    {Host1, Port1} = map_host(Client, {Host0, Port0}),
    NewChannelName = new_channel_name(Client, Host1, Port1),
    NewUrlMap = maps:merge(ServerURLMap, #{
        host => Host1,
        port => Port1
    }),
    NewRPCOptions = maps:merge(RPCOptions, RPCOptionsOverrides),
    case start_channel(SupRef, NewChannelName, NewUrlMap, NewRPCOptions, ReapChannel) of
        ok ->
            NewClient = Client#{
                channel => NewChannelName,
                url_maps => [NewUrlMap],
                url => uri_string:recompose(NewUrlMap),
                rpc_options => NewRPCOptions,
                reap_channel => ReapChannel,
                grpc_timeout => GRPCTimeout
            },
            case echo(NewClient) of
                ok ->
                    {ok, NewClient};
                {error, Reason} ->
                    ?LOG_ERROR(
                        "[hstreamdb] ping new connection failed: ~p,~nurl: ~p, channel: ~p~n",
                        [Reason, uri_string:recompose(NewUrlMap), NewChannelName]
                    ),
                    _ = stop(NewClient),
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

-spec stop(t()) -> ok.
stop(#{sup_ref := SupRef, channel := Channel}) ->
    stop_channel(SupRef, Channel).

stop_channel(SupRef, Channel) ->
    case stop_channel_pool(SupRef, Channel) of
        ok ->
            ?LOG_DEBUG("[hstreamdb] stop_channel ok: ~p", [Channel]);
        {error, _} = Error ->
            ?LOG_ERROR("[hstreamdb] stop_channel error: ~p, ~p", [Channel, Error])
    end.

stop_channel_pool(undefined, Channel) ->
    grpc_client_sup:stop_channel_pool(Channel);
stop_channel_pool(SupRef, Channel) ->
    hstreamdb_grpc_sup:stop_supervised(SupRef, Channel).

-spec name(t()) -> name().
name(#{channel := Channel}) ->
    Channel.

-spec echo(t()) -> ok | {error, term()}.
echo(Client) ->
    do_echo(Client).

-spec create_stream(t(), name(), replication_factor(), backlog_duration(), shard_count()) ->
    ok | {error, term()}.
create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount) ->
    do_create_stream(Client, Name, ReplFactor, BacklogDuration, ShardCount).

-spec delete_stream(t(), name()) -> ok | {error, term()}.
delete_stream(Client, Name) ->
    delete_stream(Client, Name, true, true).

-spec delete_stream(t(), name(), boolean(), boolean()) -> ok | {error, term()}.
delete_stream(Client, Name, IgnoreNonExist, Force) ->
    do_delete_stream(Client, Name, IgnoreNonExist, Force).

-spec lookup_shard(t(), shard_id()) -> {ok, addr()} | {error, term()}.
lookup_shard(Client, ShardId) ->
    do_lookup_shard(Client, ShardId).

-spec list_shards(t(), stream()) -> {ok, [shard()]} | {error, term()}.
list_shards(Client, StreamName) ->
    do_list_shards(Client, StreamName).

-spec append(t(), append_req()) -> ok | {error, term()}.
append(Client, Req) ->
    append(Client, Req, #{}).

-spec append(t(), append_req(), grpc_req_options()) -> {ok, append_res()} | {error, term()}.
append(Client, Req, Options) ->
    do_append(Client, Req, Options).

-spec lookup_resource(t(), resource_type(), resource_id()) -> {ok, addr()} | {error, term()}.
lookup_resource(Client, ResourceType, ResourceId) ->
    do_lookup_resource(Client, ResourceType, ResourceId).

-spec lookup_key(t(), partitioning_key()) -> {ok, addr()} | {error, term()}.
lookup_key(Client, Key) ->
    do_lookup_key(Client, Key).

-spec read_single_shard_stream(t(), stream(), limits_shard()) -> {ok, gstream()} | {error, term()}.
read_single_shard_stream(Client, StreamName, Limits) ->
    do_read_single_shard_stream(Client, StreamName, Limits).

-spec read_shard_gstream(t(), shard_id(), limits_shard()) -> {ok, gstream()} | {error, term()}.
read_shard_gstream(Client, ShardId, Limits) ->
    do_read_shard_gstream(Client, ShardId, Limits).

-spec read_key_gstream(t()) -> {ok, gstream()} | {error, term()}.
read_key_gstream(Client) ->
    do_read_key_gstream(Client).

-spec fold_shard_read_gstream(gstream(), reader_fold_fun(), reader_fold_acc()) -> reader_fold_acc().
fold_shard_read_gstream(GStream, Fun, Acc) ->
    do_fold_shard_read_gstream(GStream, Fun, Acc).

-spec fold_key_read_gstream(
    gstream(), stream(), partitioning_key(), limits_key(), reader_fold_fun(), reader_fold_acc()
) -> reader_fold_acc().
fold_key_read_gstream(GStream, Stream, Key, Limits, Fun, Acc) ->
    do_fold_key_read_gstream(GStream, Stream, Key, Limits, Fun, Acc).

-spec trim(t(), stream(), [hrecord_id()]) -> {ok, #{shard_id() => trim_point()}} | {error, term()}.
trim(#{grpc_timeout := Timeout} = Client, Stream, Offsets) ->
    do_trim(Client, Stream, Offsets, Timeout).

-spec trim(t(), stream(), [hrecord_id()], pos_integer()) ->
    {ok, #{shard_id() => trim_point()}} | {error, term()}.
trim(Client, Stream, Offsets, Timeout) ->
    do_trim(Client, Stream, Offsets, Timeout).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Channel creation

start_channel(SupRef, ChannelName, URLMap, RPCOptions, ReapChannel) ->
    URL = uri_string:recompose(URLMap),
    case create_channel_pool(SupRef, ReapChannel, ChannelName, URL, RPCOptions) of
        {ok, _, _} ->
            ?LOG_DEBUG("[hstreamdb] start_channel ok: ~p, ~p", [ChannelName, URL]);
        {ok, _} ->
            ?LOG_DEBUG("[hstreamdb] start_channel ok: ~p, ~p", [ChannelName, URL]);
        {error, _} = Error ->
            ?LOG_ERROR("[hstreamdb] start_channel error: ~p, ~p, ~p", [ChannelName, URL, Error]),
            Error
    end.

create_channel_pool(undefined, false, ChannelName, URL, RPCOptions) ->
    grpc_client_sup:create_channel_pool(ChannelName, URL, RPCOptions);
create_channel_pool(SupRef, ReapChannel, ChannelName, URL, RPCOptions) ->
    Pid =
        case ReapChannel of
            true -> self();
            false -> undefined
        end,
    hstreamdb_grpc_sup:start_supervised(SupRef, ChannelName, URL, RPCOptions, Pid).

%% GRPC methods

do_echo(Client) ->
    case ?HSTREAMDB_GEN_CLIENT:echo(#{msg => ?ECHO_MESSAGE}, metadata(Client), options(Client)) of
        {ok, #{msg := ?ECHO_MESSAGE}, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_create_stream(
    Client,
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
    case ?HSTREAMDB_GEN_CLIENT:create_stream(Req, metadata(Client), options(Client)) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_delete_stream(
    Client,
    Name,
    IgnoreNonExist,
    Force
) ->
    Req = #{
        streamName => Name,
        ignoreNonExist => IgnoreNonExist,
        force => Force
    },
    case ?HSTREAMDB_GEN_CLIENT:delete_stream(Req, metadata(Client), options(Client)) of
        {ok, _, _} ->
            ok;
        {error, R} ->
            {error, R}
    end.

do_lookup_shard(Client, ShardId) ->
    Req = #{shardId => ShardId},
    case ?HSTREAMDB_GEN_CLIENT:lookup_shard(Req, metadata(Client), options(Client)) of
        {ok, #{serverNode := #{host := Host, port := Port}}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

do_list_shards(Client, StreamName) ->
    Req = #{streamName => StreamName},
    case ?HSTREAMDB_GEN_CLIENT:list_shards(Req, metadata(Client), options(Client)) of
        {ok, #{shards := Shards}, _} ->
            ?LOG_DEBUG("[hstreamdb] fetched shards for stream ~p:~n~p", [StreamName, Shards]),
            {ok, Shards};
        {error, _} = Error ->
            Error
    end.

do_append(
    #{channel := Channel, grpc_timeout := Timeout} = Client,
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
            Options = maps:merge(options(Client), OptionOverrides),
            case
                timer:tc(fun() -> ?HSTREAMDB_GEN_CLIENT:append(NReq, metadata(Client), Options) end)
            of
                {Time, {ok, Resp, _MetaData}} ->
                    ?LOG_DEBUG(
                        "[hstreamdb] flush_request[~p, ~p], pid=~p,~nSUCCESS in ~p ms, ~p records",
                        [
                            Channel, ShardId, self(), Time div 1000, length(Records)
                        ]
                    ),
                    {ok, Resp};
                {Time, {error, Reason}} ->
                    ?LOG_ERROR(
                        "flush_request[~p, ~p], pid=~p, timeout=~p in ~p ms,~nERROR: ~p", [
                            Channel, ShardId, self(), Timeout, Time div 1000, Reason
                        ]
                    ),
                    {error, Reason}
            end;
        {error, R} ->
            {error, R}
    end.

do_lookup_resource(
    Client,
    ResourceType,
    ResourceId
) ->
    Req = #{resType => ResourceType, resId => ResourceId},
    case ?HSTREAMDB_GEN_CLIENT:lookup_resource(Req, metadata(Client), options(Client)) of
        {ok, #{host := Host, port := Port}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

do_lookup_key(
    Client,
    Key
) ->
    Req = #{partitionKey => Key},
    case ?HSTREAMDB_GEN_CLIENT:lookup_key(Req, metadata(Client), options(Client)) of
        {ok, #{host := Host, port := Port}, _} ->
            {ok, {Host, Port}};
        {error, Error} ->
            {error, Error}
    end.

do_read_single_shard_stream(
    Client,
    StreamName,
    Opts
) ->
    Req0 = #{
        streamName => StreamName,
        readerId => integer_to_binary(erlang:unique_integer([positive]))
    },
    Limits = maps:get(limits, Opts, #{}),
    Req = maps:merge(Req0, Limits),
    Options = options(Client),
    ?LOG_DEBUG("[hstreamdb] read_single_shard: Req: ~p~nOptions: ~p~n", [Req, Options]),
    case ?HSTREAMDB_GEN_CLIENT:read_single_shard_stream(metadata(Client), Options) of
        {ok, GStream} ->
            ok = grpc_client:send(GStream, Req, fin),
            {FoldFun, Acc} = maps:get(fold, Opts, {fun append_rec/2, []}),
            do_fold_shard_read_gstream(GStream, FoldFun, Acc);
        {error, _} = Error ->
            Error
    end.

do_read_shard_gstream(
    Client,
    ShardId,
    Limits
) ->
    Req0 = #{
        shardId => ShardId,
        readerId => integer_to_binary(erlang:unique_integer([positive]))
    },
    Req = maps:merge(Req0, Limits),
    case ?HSTREAMDB_GEN_CLIENT:read_shard_stream(metadata(Client), options(Client)) of
        {ok, GStream} ->
            ok = grpc_client:send(GStream, Req, fin),
            {ok, GStream};
        {error, _} = Error ->
            Error
    end.

do_read_key_gstream(Client) ->
    case ?HSTREAMDB_GEN_CLIENT:read_stream_by_key(metadata(Client), options(Client)) of
        {ok, GStream} ->
            {ok, GStream};
        {error, _} = Error ->
            Error
    end.

do_fold_key_read_gstream(GStream, Stream, Key, Limits0, Fun, Acc) ->
    {TotalLeft, Limits2} =
        case maps:take(readRecordCount, Limits0) of
            error ->
                {infinity, Limits0};
            {Value, Limits1} ->
                {Value, Limits1}
        end,
    Req0 = #{
        streamName => Stream,
        readerId => integer_to_binary(erlang:unique_integer([positive])),
        key => Key
    },
    Req1 = maps:merge(Req0, Limits2),
    fold_key_read_gstream_rounds(GStream, Req1, TotalLeft, Fun, Acc).

%% Helper functions

append_rec(eos, Acc) -> lists:reverse(Acc);
append_rec(Rec, Acc) -> [Rec | Acc].

validate_reap_sup_ref(true, undefined, _Fun) ->
    {error, cannot_reap_channel_without_sup_ref};
validate_reap_sup_ref(_Reap, _SupRef, Fun) ->
    Fun().

validate_urls_and_opts(URLs, GunOpts0, Fun) ->
    SplitURLs = string:split(URLs, ",", all),
    case SplitURLs of
        [""] ->
            {error, {invalid_url, "empty", URLs}};
        [URL | Other] ->
            %% We infer scheme and options from the first URL
            case validate_url_and_opts(URL, GunOpts0) of
                {ok, URLMap, GunOpts1} ->
                    case parse_other_hosts(Other, URLMap, [URLMap]) of
                        {ok, URLMaps} ->
                            Fun(URLMaps, GunOpts1);
                        {error, _} = Error ->
                            Error
                    end;
                {error, _} = Error ->
                    Error
            end
    end.

parse_other_hosts([], _BaseUrlMap, Acc) ->
    {ok, lists:reverse(Acc)};
parse_other_hosts([HostPort | Rest], BaseUrlMap, Acc) ->
    case parse_host_port(HostPort) of
        {ok, Host, Port} ->
            parse_other_hosts(Rest, BaseUrlMap, [
                maps:merge(BaseUrlMap, #{
                    host => Host,
                    port => Port
                })
                | Acc
            ]);
        {error, _} = Error ->
            Error
    end.

parse_host_port(HostPort) ->
    case string:split(HostPort, ":", all) of
        [Host] ->
            {ok, Host, ?DEFAULT_HSTREAMDB_PORT};
        [Host, PortStr] ->
            case string:to_integer(PortStr) of
                {Port, ""} ->
                    {ok, Host, Port};
                _ ->
                    {error, {invalid_port, PortStr}}
            end;
        _ ->
            {error, {invalid_host_port, HostPort}}
    end.

validate_url_and_opts(URL, GunOpts) ->
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

map_host(#{host_mapping := HostMapping} = _Client, {Host, Port}) ->
    case maps:find({Host, Port}, HostMapping) of
        {ok, {Host1, Port1}} ->
            {host_to_string(Host1), Port1};
        error ->
            {host_to_string(Host), Port}
    end.

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
to_channel_name(Tuple) when is_tuple(Tuple) ->
    lists:flatten(lists:join("_", [to_channel_name(El) || El <- tuple_to_list(Tuple)]));
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
            case fold_results(Results, Fun, Acc) of
                {ok, NewAcc} ->
                    do_fold_shard_read_gstream(GStream, Fun, NewAcc);
                {stop, NewAcc} ->
                    {ok, NewAcc}
            end;
        {error, _} = Error ->
            Error
    end.

fold_results([], _Fun, Acc) ->
    {ok, Acc};
fold_results([Result | Rest], Fun, Acc) ->
    case fold_result(Result, Fun, Acc) of
        {ok, NewAcc} ->
            fold_results(Rest, Fun, NewAcc);
        {stop, NewAcc} ->
            {stop, NewAcc}
    end.

fold_result(#{receivedRecords := Records}, Fun, Acc) ->
    {ok, fold_batch_records(Records, Fun, Acc)};
fold_result({eos, _}, Fun, Acc) ->
    {stop, Fun(eos, Acc)}.

fold_batch_records([], _Fun, Acc) ->
    Acc;
fold_batch_records([Record | Rest], Fun, Acc) ->
    NewAcc = fold_batch_record(Record, Fun, Acc),
    fold_batch_records(Rest, Fun, NewAcc).

fold_batch_record(#{record := _, recordIds := [#{batchId := BatchId} | _]} = BatchRecord, Fun, Acc) ->
    Records = decode_batch(BatchRecord),
    ?LOG_DEBUG("[hstreamdb] fold_batch_record, id: ~p,~nrecords: ~p~n", [BatchId, length(Records)]),
    fold_hstream_records(Records, Fun, Acc).

fold_hstream_records(Records, Fun, Acc) ->
    lists:foldl(Fun, Acc, Records).

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

fold_key_read_gstream_rounds(GStream, Req0, 0, Fun, Acc) ->
    Req = Req0#{readRecordCount => 0},
    ?LOG_DEBUG("fold_key_read_gstream send: ~p~n", [Req]),
    ok = grpc_client:send(GStream, Req, fin),
    %% We received all records we need, but not eos.
    %% So we need to send a new request to get the eos.
    case grpc_client:recv(GStream) of
        {ok, [{eos, _Trails}]} -> ok;
        %% this is probably a server error, but we do not care,
        %% we received all records we need anyway.
        {error, not_found} -> ok
    end,
    {ok, Fun(eos, Acc)};
fold_key_read_gstream_rounds(GStream, Req, TotalLeft, Fun, Acc) ->
    {NewTotalLeft, StepCount} = read_record_count(TotalLeft, ?READ_KEY_STEP_COUNT),
    Req1 = Req#{readRecordCount => StepCount},
    ?LOG_DEBUG("fold_key_read_gstream send: ~p~n", [Req1]),
    ok = grpc_client:send(GStream, Req1),
    case fold_key_read_gstream_round(GStream, StepCount, Fun, Acc) of
        {ok, NewAcc} ->
            fold_key_read_gstream_rounds(GStream, Req, NewTotalLeft, Fun, NewAcc);
        {stop, NewAcc} ->
            {ok, NewAcc};
        {error, _} = Error ->
            Error
    end.

fold_key_read_gstream_round(_GStream, Count, _Fun, Acc) when Count == 0 ->
    {ok, Acc};
fold_key_read_gstream_round(GStream, Count, Fun, Acc) ->
    case grpc_client:recv(GStream) of
        {ok, RoundSubResults} ->
            ?LOG_DEBUG("fold_key_read_gstream recv: ~p~n", [RoundSubResults]),
            case fold_key_read_gstream_round_sub_results(0, RoundSubResults, Fun, Acc) of
                {stop, NewAcc} ->
                    {stop, NewAcc};
                {N, NewAcc} ->
                    fold_key_read_gstream_round(GStream, Count - N, Fun, NewAcc)
            end;
        {error, _} = Error ->
            ?LOG_DEBUG("fold_key_read_gstream recv error: ~p~n", [Error]),
            Error
    end.

fold_key_read_gstream_round_sub_results(N, [], _Fun, Acc) ->
    {N, Acc};
fold_key_read_gstream_round_sub_results(_N, [{eos, _} | _Rest], Fun, Acc) ->
    {stop, Fun(eos, Acc)};
fold_key_read_gstream_round_sub_results(N, [RoundSubResult | Rest], Fun, Acc) ->
    Recs = merge_round_sub_res(RoundSubResult),
    NewAcc = lists:foldl(Fun, Acc, Recs),
    fold_key_read_gstream_round_sub_results(N + length(Recs), Rest, Fun, NewAcc).

merge_round_sub_res(#{receivedRecords := Recs, recordIds := RecIds}) ->
    lists:zipwith(
        fun(Rec, RecId) ->
            Rec#{recordId => RecId}
        end,
        Recs,
        RecIds
    ).

read_record_count(infinity, StepCount) ->
    {infinity, StepCount};
read_record_count(TotalLeft, StepCount) ->
    NewStepCount = min(TotalLeft, StepCount),
    {TotalLeft - NewStepCount, NewStepCount}.

do_trim(Client, Stream, Offsets, Timeout) ->
    RecordIds = format_offsets(Offsets),
    Req = #{streamName => Stream, recordIds => RecordIds},
    Options = maps:put(timeout, Timeout, options(Client)),
    case ?HSTREAMDB_GEN_CLIENT:trim_shards(Req, metadata(Client), Options) of
        {ok, #{trimPoints := TrimPoints}, _} ->
            {ok, TrimPoints};
        {error, _} = Error ->
            Error
    end.

format_offsets(Offsets) ->
    lists:map(fun format_offset/1, Offsets).

format_offset(#{shardId := ShardId, batchId := BatchId, batchIndex := BatchIndex}) ->
    <<
        (integer_to_binary(ShardId))/binary,
        "-",
        (integer_to_binary(BatchId))/binary,
        "-",
        (integer_to_binary(BatchIndex))/binary
    >>.

shuffle(List) ->
    {_, ShuffledList} = lists:unzip(lists:sort([{rand:uniform(), X} || X <- List])),
    ShuffledList.

metadata(#{metadata := Metadata} = _Client) ->
    maps:map(
        fun
            (_K, V) when is_function(V, 0) ->
                V();
            (_K, V) ->
                V
        end,
        Metadata
    ).

options(#{channel := Channel, grpc_timeout := Timeout} = _Client) ->
    #{channel => Channel, timeout => Timeout}.

prepare_metadata(Options) ->
    Metadata = maps:get(metadata, Options, #{}),
    case Options of
        #{username := Username, password := Password} ->
            Metadata#{<<"authorization">> => authorization(Username, Password)};
        _ ->
            Metadata
    end.

authorization(Username, Password) ->
    UsernameBin = str_to_bin(Username),
    PasswordBin = str_to_bin(Password),
    Token = <<UsernameBin/binary, ":", PasswordBin/binary>>,
    TokenBase64 = base64:encode(Token),
    HeaderValue = <<"Basic ", TokenBase64/binary>>,
    fun() -> HeaderValue end.

str_to_bin(B) when is_binary(B) -> B;
str_to_bin(L) when is_list(L) -> unicode:characters_to_binary(L).
