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

-module(hstreamdb_backoff).

-export([
    new/1,
    next_delay/1
]).

-export_type([
    options/0,
    t/0
]).

-type delay() :: number().
-type max_delay() :: number().
-type multiplier() :: number().

-type options() :: {delay(), max_delay(), multiplier()}.

-type t() :: #{
    delay := number(),
    max_delay := number(),
    multiplier := number()
}.

-define(IS_NUM_GT(N, MIN), (is_number(N) andalso N > MIN)).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(options()) -> t().
new({BaseDelay, MaxDelay, Multiplier}) when
    ?IS_NUM_GT(BaseDelay, 0) andalso
        ?IS_NUM_GT(MaxDelay, 0) andalso
        ?IS_NUM_GT(Multiplier, 1.0)
->
    #{
        delay => BaseDelay,
        max_delay => ceil(MaxDelay),
        multiplier => Multiplier
    }.

-spec next_delay(t()) -> {delay(), t()}.
next_delay(
    #{
        delay := BaseDelay,
        max_delay := MaxDelay,
        multiplier := Multiplier
    } = Backoff
) ->
    case BaseDelay of
        Delay when Delay > MaxDelay ->
            {MaxDelay, Backoff};
        Delay ->
            {ceil(Delay), Backoff#{delay => Delay * Multiplier}}
    end.
