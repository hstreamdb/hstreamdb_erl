-module(hstreamdb_erlang_producer_v2).

-behaviour(gen_server).

-export([init/1]).

% --------------------------------------------------------------------------------

init(
    #{
        producer_option := ProducerOption
    } = _Args
) ->
    ProducerStatus = neutral_producer_status(),
    State = build_producer_state(ProducerStatus, ProducerOption),
    {ok, State}.

% --------------------------------------------------------------------------------

start(
    #{
        producer_option := _ProducerOption
    } = Args
) ->
    gen_server:start(?MODULE, Args, []).

start_link(
    #{
        producer_option := _ProducerOption
    } = Args
) ->
    gen_server:start_link(?MODULE, Args, []).

% --------------------------------------------------------------------------------

build_batch_setting(BatchSetting) when is_tuple(BatchSetting) ->
    {BatchSettingAtom, BatchSettingValue} = BatchSetting,
    BatchSettingMap = #{BatchSettingAtom => BatchSettingValue},
    build_batch_setting(BatchSettingMap);
build_batch_setting(BatchSetting) when is_list(BatchSetting) ->
    BatchSettingMap = maps:from_list(BatchSetting),
    build_batch_setting(BatchSettingMap);
build_batch_setting(BatchSetting) when is_map(BatchSetting) ->
    Get = fun(X) -> maps:get(X, BatchSetting, undefined) end,
    RecordCountLimit = Get(record_count_limit),
    BytesLimit = Get(bytes_limit),
    AgeLimit = Get(age_limit),
    build_batch_setting(RecordCountLimit, BytesLimit, AgeLimit).

check_batch_setting(RecordCountLimit, BytesLimit, AgeLimit) ->
    BatchSettingList = [RecordCountLimit, BytesLimit, AgeLimit],
    try
        true = lists:any(fun(X) -> X /= undefined end, BatchSettingList)
    catch
        error:_ ->
            throw(hstreamdb_exception)
    end.

build_batch_setting(RecordCountLimit, BytesLimit, AgeLimit) ->
    check_batch_setting(RecordCountLimit, BytesLimit, AgeLimit),
    #{
        record_count_limit => RecordCountLimit,
        bytes_limit => BytesLimit,
        age_limit => AgeLimit
    }.

build_producer_option(ServerUrl, StreamName, BatchSetting) ->
    #{
        server_url => ServerUrl,
        stream_name => StreamName,
        batch_setting => BatchSetting
    }.

build_batch_status(RecordCount, Bytes) ->
    #{
        record_count => RecordCount,
        bytes => Bytes
    }.

build_producer_status(Records, BatchStatus) ->
    #{
        records => Records,
        batch_status => BatchStatus
    }.

build_producer_state(ProducerStatus, ProducerOption) ->
    #{
        producer_status => ProducerStatus,
        producer_option => ProducerOption
    }.

neutral_batch_status() ->
    build_batch_status(0, 0).

neutral_producer_status() ->
    build_producer_status([], neutral_batch_status()).

% --------------------------------------------------------------------------------
