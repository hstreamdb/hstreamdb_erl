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

-export([
    new/1,
    append/4,
    flush/1,
    is_empty/1
]).

-export([handle_event/2]).

-export_type([hstreamdb_buffer/0, options/0]).

-type options() :: #{
    flush_interval := pos_integer(),
    batch_timeout := pos_integer(),
    batch_size := pos_integer(),
    batch_max_count := pos_integer(),
    batch_tab := ets:tab(),

    send_batch := fun((batch_message()) -> any()),

    send_after := fun((pos_integer(), term()) -> reference()),
    cancel_send := fun((reference()) -> any()),
    send_reply := fun((term(), ok | {error, term()}) -> any())
}.

-type hstreamdb_buffer() :: #{
    flush_interval := pos_integer(),
    batch_timeout := pos_integer(),
    batch_size := pos_integer(),
    batch_max_count := pos_integer(),
    current_batch := [internal_record()],
    batch_tab := ets:table(),
    batch_queue := queue:queue(),
    batch_count := pos_integer(),
    inflight_batch_ref := reference() | undefined,

    batch_timer := timer:timer_ref(),
    flush_timer := timer:timer_ref(),
    send_batch := fun((batch_message()) -> any()),
    send_after := fun((pos_integer(), term()) -> reference()),
    cancel_send := fun((reference()) -> any()),
    send_reply := fun((from(), ok | {error, term()}) -> any())
}.

-type from() :: term().

-type record() :: term().

-type internal_record() :: #{
    from := from() | fun((ok | {error, term()}) -> any()),
    data := record(),
    deadline := timeout()
}.

-type batch_message() :: #{
    batch_ref => reference(),
    tab => ets:tab()
}.

-type timeout_event() :: {batch_timeout, reference()} | flush.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> hstreamdb_buffer().
new(#{
    flush_interval := FlushInterval,
    batch_timeout := BatchTimeout,
    batch_tab := BatchTab,
    batch_size := BatchSize,
    batch_max_count := BatchMaxCount,

    send_batch := SendBatch,
    send_after := SendAfter,
    cancel_send := CancelSend,
    send_reply := SendReply
}) ->
    #{
        flush_interval => FlushInterval,
        batch_timeout => BatchTimeout,
        current_batch => [],
        batch_tab => BatchTab,
        batch_size => BatchSize,
        batch_max_count => BatchMaxCount,

        batch_queue => queue:new(),
        batch_count => 0,
        inflight_batch_ref => undefined,
        flush_timer => undefined,
        batch_timer => undefined,

        send_batch => SendBatch,
        send_after => SendAfter,
        cancel_send => CancelSend,
        send_reply => SendReply
    }.

-define(IS_TIMEOUT(Timeout),
    ((is_integer(Timeout) andalso Timeout > 0) orelse Timeout == infinity)
).

-spec append(hstreamdb_buffer(), [record()], from(), timeout()) ->
    {ok, hstreamdb_buffer()} | {error, term()}.
append(#{batch_max_count := BatchMaxCount} = Buffer, From, Records, Timeout) when
    ?IS_TIMEOUT(Timeout) andalso is_list(Records)
->
    case estimate_batch_count(Buffer, Records) of
        NewBatchCount when NewBatchCount > BatchMaxCount ->
            {error, {batch_count_too_large, NewBatchCount}};
        _NewBatchCount ->
            InternalRecords = to_internal_records(Records, From, Timeout),
            do_add_records(Buffer, InternalRecords)
    end.

-spec flush(hstreamdb_buffer()) -> hstreamdb_buffer().
flush(#{current_batch := []} = Buffer) ->
    cancel_timer(flush_timer, Buffer);
flush(Buffer0) ->
    Buffer1 = cancel_timer(flush_timer, Buffer0),
    Buffer2 = store_current_batch(Buffer1),
    Buffer3 = maybe_send_batch(Buffer2),
    reschedule_flush(Buffer3).

-spec handle_event(hstreamdb_buffer(), timeout_event()) -> hstreamdb_buffer().
%% Flush event
handle_event(Buffer, flush) ->
    flush(Buffer);
%% Batch timeout event
handle_event(#{inflight_batch_ref := BatchRef} = Buffer0, {batch_timeout, BatchRef}) ->
    Buffer1 = send_timeout_responses(Buffer0, BatchRef),
    maybe_send_batch(Buffer1#{inflight_batch_ref => undefined});
%% May happen due to concurrency
handle_event(Buffer, {batch_timeout, _BatchRef}) ->
    Buffer;
handle_event(#{inflight_batch_ref := BatchRef} = Buffer0, {batch_response, BatchRef, Response}) ->
    Buffer1 = send_responses(Buffer0, BatchRef, Response),
    maybe_send_batch(Buffer1#{inflight_batch_ref => undefined});
%% Late response, the batch has been reaped by timeout timer
handle_event(Buffer, {batch_response, _BatchRef, _Response}) ->
    Buffer.

-spec is_empty(hstreamdb_buffer()) -> boolean().
is_empty(#{current_batch := [], inflight_batch_ref := undefined, batch_count := 0}) ->
    true;
is_empty(_Buffer) ->
    false.

send_timeout_responses(Buffer, BatchRef) ->
    send_responses(Buffer, BatchRef, {error, timeout}).

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
send_responses_to_records(Buffer, [#{deadline := Deadline} | Records], Response, Now) when
    Deadline < Now
->
    send_responses_to_records(Buffer, Records, Response, Now);
send_responses_to_records(Buffer, [#{from := From} | Records], Response, Now) when
    is_function(From)
->
    _ = apply_reply_fun(From, [Response]),
    send_responses_to_records(Buffer, Records, Response, Now);
send_responses_to_records(
    #{send_reply := SendReplyFun} = Buffer, [#{from := From} | Records], Response, Now
) ->
    _ = apply_reply_fun(SendReplyFun, [From, Response]),
    send_responses_to_records(Buffer, Records, Response, Now).

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
                maybe_schedule_flush(Buffer1);
            _ ->
                Buffer2 = store_batches(Buffer1, NewBatches),
                Buffer3 = maybe_send_batch(Buffer2),
                reschedule_flush(Buffer3)
        end,
    {ok, BufferNew}.

update_current_batch(
    #{current_batch := CurrentBatch, batch_size := BatchSize} = Buffer, Records
) when length(CurrentBatch) + length(Records) < BatchSize ->
    {[], Buffer#{current_batch := append_current_batch(Records, CurrentBatch)}};
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

maybe_send_batch(#{inflight_batch_ref := undefined, batch_count := 0} = Buffer0) ->
    cancel_timer(batch_timer, Buffer0);
maybe_send_batch(
    #{inflight_batch_ref := undefined, batch_count := BatchCount, batch_timeout := BatchTimeout} =
        Buffer0
) when BatchCount > 0 ->
    Buffer1 = cancel_timer(batch_timer, Buffer0),
    {BatchRef, Buffer2} = send_batch(Buffer1),
    send_after(batch_timer, Buffer2, BatchTimeout, {batch_timeout, BatchRef});
maybe_send_batch(#{inflight_batch_ref := _BatchRef} = Buffer) ->
    Buffer.

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

send_batch(
    #{
        inflight_batch_ref := undefined,
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
    ok = SendBatchFun(Message),
    {BatchRef, Buffer#{
        inflight_batch_ref => BatchRef, batch_queue => NewBatchQueue, batch_count => BatchCount - 1
    }}.

to_batches([], _, Batches) ->
    lists:reverse(Batches);
to_batches(Records, BatchSize, Batches) ->
    {Batch, Left} = lists:split(BatchSize, Records),
    to_batches(Left, BatchSize, [Batch | Batches]).

to_internal_records(Records, From, Timeout) ->
    Deadline = deadline(Timeout),
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

deadline(infinity) ->
    infinity;
deadline(Timeout) ->
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
            logger:warning("[hstreamdb]: Error in reply function: ~p:~p~n", [Error, Reason])
    end.
