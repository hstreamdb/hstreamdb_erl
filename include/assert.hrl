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

-include_lib("stdlib/include/assert.hrl").

-define(DEFAULT_RECEIVE_TIMEOUT, 200).

-define(assertReceive(Pattern), ?assertReceive(Pattern, ?DEFAULT_RECEIVE_TIMEOUT)).
-define(assertReceived(Pattern), ?assertReceive(Pattern, 0)).

-define(assertReceive(Pattern, Timeout),
    receive
        Pattern -> ok
    after Timeout ->
        ?assert(false, "Expected message not received")
    end
).

-define(refuteReceive(Pattern), ?refuteReceive(Pattern, ?DEFAULT_RECEIVE_TIMEOUT)).
-define(refuteReceived(Pattern), ?refuteReceive(Pattern, 0)).

-define(refuteReceive(Pattern, Timeout),
    receive
        Pattern -> ?assert(false, "Unexpected message received")
    after Timeout ->
        ok
    end
).

-define(assertWaitEvent(Code, EventMatch, Timeout),
    ?assertMatch(
        {_, {ok, EventMatch}},
        ?wait_async_action(
            Code,
            EventMatch,
            Timeout
        )
    )
).
