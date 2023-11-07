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

-module(hstreamdb_buffer).

-include("hstreamdb.hrl").
-include("errors.hrl").
-include_lib("kernel/include/logger.hrl").

-export([
    new/1,
    append/2,
    flush/1,
    drop/2,
    drop_inflight/2,
    is_empty/1,

    to_buffer_records/1,
    to_buffer_records/3,
    deadline/1,
    data/1
]).

-export([
    handle_event/3,
    handle_batch_response/4
]).

-export_type([hstreamdb_buffer/0, options/0]).

-type batch_ref() :: reference().
-type req_ref() :: reference().
-type timer_ref() :: reference().

-type send_batch_fun() :: fun((batch_message()) -> batch_ref()).
-type send_after_fun() :: fun((pos_integer(), term()) -> timer_ref()).
-type cancel_send_fun() :: fun((timer_ref()) -> any()).
-type send_reply_fun() :: fun((from(), ok | {error, term()}) -> any()).

-type options() :: #{
    flush_interval := pos_integer(),
    batch_timeout := pos_integer(),
    batch_size := pos_integer(),
    batch_max_count := pos_integer(),
    batch_tab := ets:tab(),

    backoff_options => hstreamdb_backoff:options(),
    max_retries => non_neg_integer(),

    send_batch := send_batch_fun(),
    send_after := send_after_fun(),
    cancel_send := cancel_send_fun(),
    send_reply := send_reply_fun()
}.

-type hstreamdb_buffer() :: #{
    %% Incoming records
    batch_timeout := pos_integer(),
    batch_size := pos_integer(),
    batch_max_count := pos_integer(),
    current_batch := [buffer_record()],
    batch_tab := ets:table(),
    batch_queue := queue:queue(),
    batch_count := non_neg_integer(),

    %% Outgoing batches
    flush_interval := pos_integer(),
    flush_timer := reference() | undefined,
    inflight_batch := {_BatchRef :: reference(), _ReqRef :: reference()} | undefined,
    batch_timer := reference() | undefined,

    backoff_options := hstreamdb_backoff:options(),
    max_retries := non_neg_integer(),
    retry_state := undefined | {hstreamdb_backoff:t(), non_neg_integer()},

    %% Callbacks
    send_batch := send_batch_fun(),
    send_after := send_after_fun(),
    cancel_send := cancel_send_fun(),
    send_reply := send_reply_fun()
}.

-type from() :: term().

-type record() :: term().

-type buffer_record() :: #{
    from := from() | fun((ok | {error, term()}) -> any()),
    data := record(),
    deadline := timeout()
}.

-type batch_message() :: #{
    batch_ref => batch_ref(),
    tab => ets:tab()
}.

-type timeout_event() :: {batch_timeout, req_ref()} | {retry, req_ref()} | flush.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> hstreamdb_buffer().
new(
    #{
        flush_interval := FlushInterval,
        batch_timeout := BatchTimeout,
        batch_tab := BatchTab,
        batch_size := BatchSize,
        batch_max_count := BatchMaxCount,

        send_batch := SendBatch,
        send_after := SendAfter,
        cancel_send := CancelSend,
        send_reply := SendReply
    } = Options
) ->
    #{
        flush_interval => FlushInterval,
        batch_timeout => BatchTimeout,
        batch_tab => BatchTab,
        batch_size => BatchSize,
        batch_max_count => BatchMaxCount,

        batch_queue => queue:new(),
        batch_count => 0,

        current_batch => [],

        %% Input states
        %% * empty (flush_timer =/= undefined)
        %% * waiting for flush (flush_timer =/= undefined)
        flush_timer => undefined,

        %% Output states
        %% * waiting for batch response (inflight_batch =/= undefined, batch_timer =/= undefined)
        %% * waiting for batch retry (inflight_req == undefined, batch_timer =/= undefined)
        %% * idle (inflight_batch == undefined, batch_timer === undefined)
        inflight_batch => undefined,
        batch_timer => undefined,
        backoff_options => maps:get(backoff_options, Options, ?DEFAULT_BATCH_BACKOFF_OPTIONS),
        max_retries => maps:get(max_retries, Options, ?DEFAULT_BATCH_MAX_RETRIES),
        retry_state => undefined,

        %% Callbacks
        send_batch => SendBatch,
        send_after => SendAfter,
        cancel_send => CancelSend,
        send_reply => SendReply
    }.

-define(IS_TIMEOUT(Timeout),
    ((is_integer(Timeout) andalso Timeout > 0) orelse Timeout == infinity)
).

-spec append(hstreamdb_buffer(), [buffer_record()]) ->
    {ok, hstreamdb_buffer()} | {error, {batch_count_too_large, non_neg_integer()}}.
append(#{batch_max_count := BatchMaxCount} = Buffer, Records) ->
    case estimate_batch_count(Buffer, Records) of
        NewBatchCount when NewBatchCount > BatchMaxCount ->
            {error, {batch_count_too_large, NewBatchCount}};
        _NewBatchCount ->
            do_add_records(Buffer, Records)
    end.

-spec flush(hstreamdb_buffer()) -> hstreamdb_buffer().
flush(#{current_batch := []} = Buffer) ->
    cancel_timer(flush_timer, Buffer);
flush(Buffer0) ->
    Buffer1 = cancel_timer(flush_timer, Buffer0),
    Buffer2 = store_current_batch(Buffer1),
    Buffer3 = maybe_send_batch(Buffer2),
    reschedule_flush(Buffer3).

%% Drop all the batches except the inflight one
-spec drop(hstreamdb_buffer(), term()) -> hstreamdb_buffer().
drop(Buffer0, Reason) ->
    Buffer1 = cancel_timer(flush_timer, Buffer0),
    #{batch_queue := Queue} = Buffer2 = store_current_batch(Buffer1),
    Buffer3 = send_responses_to_batches(Buffer2, queue:to_list(Queue), {error, Reason}),
    Buffer3#{
        batch_queue := queue:new(),
        batch_count := 0
    }.

%% Drop the inflight batch
-spec drop_inflight(hstreamdb_buffer(), term()) -> hstreamdb_buffer().
drop_inflight(#{inflight_batch := {_BatchRef, ReqRef}} = Buffer, Reason) ->
    handle_batch_response(Buffer, ReqRef, {error, Reason}, false).

-spec handle_event(hstreamdb_buffer(), timeout_event(), boolean()) -> hstreamdb_buffer().
%% Flush event
handle_event(Buffer, flush, _MayRetry) ->
    flush(Buffer);
%% Batch timeout event
handle_event(#{inflight_batch := {_BatchRef, ReqRef}} = Buffer0, {batch_timeout, ReqRef}, MayRetry) ->
    handle_batch_response(Buffer0, ReqRef, {error, ?ERROR_BATCH_TIMEOUT}, MayRetry);
%% May happen due to concurrency
handle_event(Buffer, {batch_timeout, _ReqRef}, _MayRetry) ->
    Buffer;
%% Batch Retry event
handle_event(#{inflight_batch := {_BatchRef, ReqRef}} = Buffer, {retry, ReqRef}, _MayRetry) ->
    resend_batch(Buffer);
%% May happen due to concurrency
handle_event(Buffer, {retry, _ReqRef}, _MayRetry) ->
    Buffer.

-spec handle_batch_response(hstreamdb_buffer(), req_ref(), term(), boolean()) ->
    hstreamdb_buffer().
handle_batch_response(
    #{inflight_batch := {BatchRef, ReqRef}} = Buffer0, ReqRef, Response, MayRetry
) ->
    case need_retry(Buffer0, Response, MayRetry) of
        true ->
            ?LOG_DEBUG(
                "[hstreamdb] buffer, handle_batch_response, need_retry=true,~n"
                "response: ~p,~nmay_retry: ~p, retry context: ~p",
                [Response, MayRetry, retry_context(Buffer0)]
            ),
            wait_for_retry(Buffer0);
        false ->
            ?LOG_DEBUG(
                "[hstreamdb] buffer, handle_batch_response, need_retry=false,~n"
                "response: ~p,~nmay_retry: ~p, retry context: ~p",
                [Response, MayRetry, retry_context(Buffer0)]
            ),
            Buffer1 = send_responses(Buffer0, BatchRef, Response),
            Buffer2 = retry_deinit(Buffer1),
            maybe_send_batch(Buffer2#{inflight_batch => undefined})
    end;
%% Late response, the batch has been reaped or resent
handle_batch_response(Buffer, _ReqRef, _Response, _MayRetry) ->
    Buffer.

-spec is_empty(hstreamdb_buffer()) -> boolean().
is_empty(#{current_batch := [], inflight_batch := undefined, batch_count := 0}) ->
    true;
is_empty(_Buffer) ->
    false.

-spec to_buffer_records([record()]) -> [buffer_record()].
to_buffer_records(Records) when is_list(Records) ->
    to_buffer_records(Records, undefined, infinity).

-spec to_buffer_records([record()], from(), timeout()) -> [buffer_record()].
to_buffer_records(Records, From, Timeout) when ?IS_TIMEOUT(Timeout) andalso is_list(Records) ->
    Deadline = deadline_from_timeout(Timeout),
    lists:map(
        fun(Record) ->
            #{
                data => Record,
                from => From,
                deadline => Deadline
            }
        end,
        Records
    ).

-spec deadline(buffer_record()) -> timeout().
deadline(#{deadline := Deadline}) ->
    Deadline.

-spec data(buffer_record()) -> record().
data(#{data := Data}) ->
    Data.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Retries

is_transient_error(Error) ->
    %% TODO: inject
    hstreamdb_error:is_transient(Error).

need_retry(_Buffer, _Response, _MayRetry = false) ->
    false;
need_retry(
    #{retry_state := undefined, max_retries := MaxRetries}, {error, _}, _MayRetry = true
) when MaxRetries =< 1 ->
    false;
need_retry(#{retry_state := {_Backoff, RetriesLeft}} = _Buffer, {error, _}, _MayRetry = true) when
    RetriesLeft =< 0
->
    false;
need_retry(_Buffer, {error, _} = Error, _MayRetry = true) ->
    is_transient_error(Error);
need_retry(_Buffer, {ok, _}, _MayRetry = true) ->
    false.

wait_for_retry(#{retry_state := undefined} = Buffer) ->
    wait_for_retry(retry_init(Buffer));
wait_for_retry(
    #{
        retry_state := {Backoff0, RetriesLeft},
        inflight_batch := {BatchRef, _ReqRef}
    } = Buffer0
) ->
    {Delay, Backoff1} = hstreamdb_backoff:next_delay(Backoff0),
    Buffer1 = cancel_timer(batch_timer, Buffer0),
    ReqRef = make_ref(),
    Buffer2 = send_after(batch_timer, Buffer1, Delay, {retry, ReqRef}),
    Buffer2#{
        inflight_batch := {BatchRef, ReqRef},
        retry_state := {Backoff1, RetriesLeft - 1}
    }.

retry_init(#{backoff_options := BackoffOptions, max_retries := MaxRetries} = Buffer) ->
    Backoff = hstreamdb_backoff:new(BackoffOptions),
    Buffer#{retry_state => {Backoff, MaxRetries - 1}}.

retry_deinit(Buffer) ->
    Buffer#{retry_state => undefined}.

%% Timers

cancel_timer(TimerName, #{cancel_send := CancelSendFun} = Buffer) ->
    case maps:get(TimerName, Buffer) of
        undefined ->
            Buffer;
        Timer ->
            _ = CancelSendFun(Timer),
            maps:put(TimerName, undefined, Buffer)
    end.

send_after(TimerName, #{send_after := SendAfterFun} = Buffer, Timeout, Message) ->
    undefined = maps:get(TimerName, Buffer),
    Ref = SendAfterFun(Timeout, Message),
    maps:put(TimerName, Ref, Buffer).

%%

send_responses_to_batches(Buffer, [], _Response) ->
    Buffer;
send_responses_to_batches(Buffer0, [BatchRef | BatchRefs], Response) ->
    Buffer1 = send_responses(Buffer0, BatchRef, Response),
    send_responses_to_batches(Buffer1, BatchRefs, Response).

send_responses(#{batch_tab := BatchTab} = Buffer, BatchRef, Response) ->
    Now = erlang:monotonic_time(millisecond),
    case ets:take(BatchTab, BatchRef) of
        [] ->
            Buffer;
        [{BatchRef, Records}] ->
            ok = send_responses_to_records(Buffer, Records, Response, Now),
            Buffer
    end.

send_responses_to_records(_Buffer, [], _Response, _Now) ->
    ok;
%% nobody waits for this response
send_responses_to_records(Buffer, [#{deadline := Deadline} | Records], {ok, [_ | Rest]}, Now) when
    Deadline < Now
->
    send_responses_to_records(Buffer, Records, {ok, Rest}, Now);
send_responses_to_records(Buffer, [#{deadline := Deadline} | Records], {error, _} = Error, Now) when
    Deadline < Now
->
    send_responses_to_records(Buffer, Records, Error, Now);
%% response to waiting clients: callback
send_responses_to_records(Buffer, [#{from := From} | Records], {ok, [RecordResp | Rest]}, Now) when
    is_function(From)
->
    _ = apply_reply_fun(From, [RecordResp]),
    send_responses_to_records(Buffer, Records, {ok, Rest}, Now);
send_responses_to_records(Buffer, [#{from := From} | Records], {error, _} = Error, Now) when
    is_function(From)
->
    _ = apply_reply_fun(From, [Error]),
    send_responses_to_records(Buffer, Records, Error, Now);
%% response to waiting clients: common callback
send_responses_to_records(
    #{send_reply := SendReplyFun} = Buffer,
    [#{from := From} | Records],
    {ok, [RecordResp | Rest]},
    Now
) ->
    _ = apply_reply_fun(SendReplyFun, [From, RecordResp]),
    send_responses_to_records(Buffer, Records, {ok, Rest}, Now);
send_responses_to_records(
    #{send_reply := SendReplyFun} = Buffer, [#{from := From} | Records], {error, _} = Error, Now
) ->
    _ = apply_reply_fun(SendReplyFun, [From, Error]),
    send_responses_to_records(Buffer, Records, Error, Now).

estimate_batch_count(
    #{batch_size := BatchSize, batch_count := BatchCount, current_batch := CurrentBatch}, Records
) ->
    NCurrentAndNew = length(CurrentBatch) + length(Records),
    case NCurrentAndNew rem BatchSize of
        0 ->
            BatchCount + NCurrentAndNew div BatchSize;
        _ ->
            BatchCount + NCurrentAndNew div BatchSize + 1
    end.

do_add_records(Buffer0, Records) ->
    {NewBatches, Buffer1} = update_current_batch(Buffer0, Records),

    BufferNew =
        case NewBatches of
            [] ->
                %% The records were added to the only existing batch, so
                %% just make sure the flush timer is scheduled for it
                maybe_schedule_flush(Buffer1);
            _ ->
                %% After adding the records, there are more than one batch,
                %% so we need to try to send one immediately and postpone flushing
                Buffer2 = store_batches(Buffer1, NewBatches),
                Buffer3 = maybe_send_batch(Buffer2),
                reschedule_flush(Buffer3)
        end,
    {ok, BufferNew}.

%% New records fit into the current buffer
%% [xxxx    ] current
%% + yy ->
%% [xxxxyy  ] current
update_current_batch(
    #{current_batch := CurrentBatch, batch_size := BatchSize} = Buffer, Records
) when length(CurrentBatch) + length(Records) < BatchSize ->
    {[], Buffer#{current_batch := append_current_batch(Records, CurrentBatch)}};
%% [xxxxxx  ] current
%% + yyyyyyyyyyyy ->
%% [xxxxxxyy]
%% [yyyyyyyy]
%% [yy      ] current
update_current_batch(#{current_batch := CurrentBatch, batch_size := BatchSize} = Buffer, Records) ->
    NToCurrentBatch = BatchSize - length(CurrentBatch),
    NToNewBatches = length(Records) - NToCurrentBatch,
    {ToNewCurrentAndBatches, ToCurrentBatch} = lists:split(NToNewBatches, Records),
    FilledCurrentBatch = lists:reverse(append_current_batch(ToCurrentBatch, CurrentBatch)),
    NToNewCurrentBatch = NToNewBatches rem BatchSize,
    {NewCurrentBatch, ToNewBatches} = lists:split(NToNewCurrentBatch, ToNewCurrentAndBatches),
    NewBatches = [FilledCurrentBatch | to_batches(ToNewBatches, BatchSize, [])],
    {NewBatches, Buffer#{current_batch := NewCurrentBatch}}.

store_batches(Buffer, []) ->
    Buffer;
store_batches(
    #{batch_tab := BatchTab, batch_queue := BatchQueue, batch_count := BatchCount} = Buffer, [
        Batch | Bathches
    ]
) ->
    BatchRef = make_ref(),
    StoredBatch = {BatchRef, Batch},
    true = ets:insert(BatchTab, StoredBatch),
    NewQueue = queue:in(BatchRef, BatchQueue),
    store_batches(Buffer#{batch_queue := NewQueue, batch_count := BatchCount + 1}, Bathches).

store_current_batch(#{current_batch := CurrentBatch} = Buffer0) ->
    Buffer1 = store_batches(Buffer0, [lists:reverse(CurrentBatch)]),
    Buffer1#{current_batch := []}.

reschedule_flush(#{current_batch := []} = Buffer) ->
    cancel_timer(flush_timer, Buffer);
reschedule_flush(#{current_batch := _, flush_interval := FlushInterval} = Buffer0) ->
    Buffer1 = cancel_timer(flush_timer, Buffer0),
    send_after(flush_timer, Buffer1, FlushInterval, flush).

maybe_schedule_flush(#{flush_timer := undefined} = Buffer) ->
    reschedule_flush(Buffer);
maybe_schedule_flush(Buffer) ->
    Buffer.

maybe_send_batch(#{inflight_batch := undefined, batch_count := 0} = Buffer0) ->
    cancel_timer(batch_timer, Buffer0);
maybe_send_batch(
    #{inflight_batch := undefined, batch_count := BatchCount, batch_timeout := BatchTimeout} =
        Buffer0
) when BatchCount > 0 ->
    Buffer1 = cancel_timer(batch_timer, Buffer0),
    {ReqRef, Buffer2} = send_batch(Buffer1),
    send_after(batch_timer, Buffer2, BatchTimeout, {batch_timeout, ReqRef});
maybe_send_batch(#{inflight_batch := {_BatchRef, _ReqRef}} = Buffer) ->
    Buffer.

send_batch(
    #{
        inflight_batch := undefined,
        batch_tab := BatchTab,
        send_batch := SendBatchFun,
        batch_queue := BatchQueue,
        batch_count := BatchCount
    } = Buffer
) ->
    {{value, BatchRef}, NewBatchQueue} = queue:out(BatchQueue),
    Message = #{
        batch_ref => BatchRef,
        tab => BatchTab
    },
    ReqRef = SendBatchFun(Message),
    {ReqRef, Buffer#{
        inflight_batch => {BatchRef, ReqRef},
        batch_queue => NewBatchQueue,
        batch_count => BatchCount - 1
    }}.

resend_batch(
    #{
        inflight_batch := {BatchRef, _ReqRef},
        batch_tab := BatchTab,
        batch_timeout := BatchTimeout,
        send_batch := SendBatchFun
    } = Buffer0
) ->
    Buffer1 = cancel_timer(batch_timer, Buffer0),
    Message = #{
        batch_ref => BatchRef,
        tab => BatchTab
    },
    ReqRef = SendBatchFun(Message),
    Buffer2 = send_after(batch_timer, Buffer1, BatchTimeout, {batch_timeout, ReqRef}),
    Buffer2#{
        inflight_batch := {BatchRef, ReqRef}
    }.

to_batches([], _, Batches) ->
    lists:reverse(Batches);
to_batches(Records, BatchSize, Batches) ->
    {Batch, Left} = lists:split(BatchSize, Records),
    to_batches(Left, BatchSize, [Batch | Batches]).

deadline_from_timeout(infinity) ->
    infinity;
deadline_from_timeout(Timeout) ->
    erlang:monotonic_time(millisecond) + Timeout.

append_current_batch([], CurrentBatch) ->
    CurrentBatch;
append_current_batch([Record | Records], CurrentBatch) ->
    append_current_batch(Records, [Record | CurrentBatch]).

apply_reply_fun(Fun, Args) ->
    try
        apply(Fun, Args)
    catch
        Error:Reason ->
            ?LOG_WARNING("[hstreamdb]: Error in reply function: ~p:~p", [Error, Reason])
    end.

retry_context(Buffer) ->
    maps:with(
        [
            retry_state,
            max_retries,
            backoff_options
        ],
        Buffer
    ).
