-module(hstreamdb_erlang_producer).

-behaviour(gen_statem).

-export([callback_mode/0]).
-export([start/0, start/1, start_link/0, start_link/1, init/1]).

-export([wait_for_append/3, appending/3]).

-export([readme/0]).

callback_mode() ->
    % Events are handled by one callback function per state.
    state_functions.

start_link() ->
    start_link([]).

start_link(Args) ->
    gen_statem:start_link(
        ?MODULE,
        Args,
        []
    ).

start() ->
    start([]).

start(Args) ->
    gen_statem:start(
        ?MODULE,
        Args,
        []
    ).

init(ProducerOptions) ->
    {ok, wait_for_append, #{
        recordBuffer =>
            #{
                length => 0,
                byte_size => 0,
                records => []
            },
        options => ProducerOptions
    }}.

%%--------------------------------------------------------------------

wait_for_append(
    {call, From},
    {_, EventContentMap} = EventContent,
    #{
        recordBuffer := RecordBuffer,
        options := ProducerOptions
    } = Data
) ->
    logger:notice(#{
        msg => "producer: do wait_for_append",
        val => EventContent
    }),

    Record = maps:get(record, EventContentMap),
    add_record_to_buffer(Record, RecordBuffer),

    gen_statem:reply(From, ok),
    case check_ready_for_append(get_producer_info(RecordBuffer), ProducerOptions) of
        true ->
            logger:notice(#{
                msg => "producer: wait_for_append -> appending"
            }),
            {next_state, appending, Data};
        false ->
            {next_state, wait_for_append, Data}
    end.

appending(
    {call, From},
    EventContent,
    Data
) ->
    logger:notice(#{
        msg => "producer: do appending",
        val => EventContent
    }),

    gen_statem:reply(From, ok),
    {next_state, wait_for_append, Data}.

%%--------------------------------------------------------------------

get_producer_info(XS) -> XS.

check_ready_for_append(
    #{length := RecordCount} = ProducerInfo,
    #{batchSetting := BatchSetting} = ProducerOptions
) ->
    logger:notice(#{
        msg => "producer: check_ready_for_append",
        val => ProducerOptions
    }),

    % FIXME
    RecordCountLimit = maps:get(recordCountLimit, BatchSetting, undefined),
    BytesLimit = maps:get(bytesLimit, BatchSetting, undefined),
    AgeLimit = maps:get(ageLimit, BatchSetting, undefined),

    % FIXME
    (case RecordCountLimit of
        undefined -> false;
        X when is_integer(X) -> X >= RecordCount
    end) orelse
        (case BytesLimit of
            undefined -> false;
            Y when is_integer(Y) -> Y >= RecordCount
        end) orelse
        (case AgeLimit of
            undefined -> false;
            Z when is_integer(Z) -> Z >= RecordCount
        end).

add_record_to_buffer(Record, Buffer) ->
    Buffer0 = maps:update_with(
        length, fun(X) -> X + 1 end, Buffer
    ),
    Buffer1 = maps:update_with(
        byte_size, fun(X) -> X + byte_size(Record) end, Buffer0
    ),
    Buffer2 = maps:update_with(
        records, fun(XS) -> [Record | XS] end, Buffer1
    ),
    Buffer2.

%%--------------------------------------------------------------------

readme() ->
    {ok, Pid} = start_link(#{
        batchSetting => #{
            recordCountLimit => 2
        }
    }),
    gen_statem:call(
        Pid, {append, #{record => <<"00">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"01">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"02">>}}
    ),
    ok.
