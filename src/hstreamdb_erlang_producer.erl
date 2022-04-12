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
                batchInfo => #{
                    recordCount => 0,
                    bytes => 0,
                    age => 0
                },
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
        val => [EventContent, RecordBuffer]
    }),

    Record = maps:get(record, EventContentMap),
    add_record_to_buffer(Record, RecordBuffer),

    gen_statem:reply(From, ok),
    case check_ready_for_append(RecordBuffer, ProducerOptions) of
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

check_ready_for_append(
    #{
        batchInfo := #{
            recordCount := RecordCount,
            bytes := Bytes,
            age := Age
        }
    } = ProducerInfo,
    #{
        batchSetting := BatchSetting
    } = ProducerOptions
) ->
    logger:notice(#{
        msg => "producer: check_ready_for_append",
        val => [ProducerInfo, ProducerOptions]
    }),

    BatchSettings = lists:map(
        fun(X) ->
            maps:get(X, BatchSetting, undefined)
        end,
        [
            recordCountLimit,
            bytesLimit,
            ageLimit
        ]
    ),
    true = lists:any(fun(X) -> X =/= undefined end, BatchSettings),

    lists:any(
        fun({X, XLimit}) ->
            case XLimit of
                undefined -> false;
                Limit when is_integer(Limit) -> X >= Limit
            end
        end,
        lists:zip(
            [RecordCount, Bytes, Age], BatchSettings
        )
    ).

add_record_to_buffer(
    Record,
    #{
        batchInfo := BatchInfo,
        records := Records
    } = Buffer
) ->
    Buffer0 = maps:update_with(
        recordCount, fun(X) -> X + 1 end, Buffer
    ),
    Buffer1 = maps:update_with(
        bytes, fun(X) -> X + byte_size(Record) end, Buffer0
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
