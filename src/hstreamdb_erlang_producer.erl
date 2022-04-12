-module(hstreamdb_erlang_producer).

-behaviour(gen_statem).

-export([callback_mode/0]).
-export([start_link/0, init/1]).
-export([start/0]).

-export([wait_for_append/3, appending/3]).

-export([readme/0]).

callback_mode() ->
    % Events are handled by one callback function per state.
    state_functions.

start_link() ->
    gen_statem:start_link(
        ?MODULE,
        % Args
        [],
        % Opts
        []
    ).

start() ->
    gen_statem:start(
        ?MODULE,
        % Args
        [],
        % Opts
        []
    ).

init(ProducerOptions) ->
    {ok,
        % State
        wait_for_append,
        % Data
        #{
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
    case check_ready_for_append(ProducerOptions) of
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
    ProducerOptions
) ->
    true.

add_record_to_buffer(Record, Buffer) ->
    Buffer0 = maps:update_with(
        length, fun(X) -> X + 1 end, Buffer
    ),
    Buffer1 = maps:update_with(
        byte_size, fun(X) -> X + 1 end, Buffer0
    ),
    Buffer2 = maps:update_with(
        records, fun(XS) -> [Record | XS] end, Buffer1
    ),
    Buffer2.

%%--------------------------------------------------------------------

readme() ->
    {ok, Pid} = start_link(),
    gen_statem:call(
        Pid, {append, #{record => <<"00">>}}
    ),
    gen_statem:call(
        Pid, {append, #{record => <<"01">>}}
    ),
    ok.
