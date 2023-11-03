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

-module(hstreamdb_error).

-include("errors.hrl").

-export([
    is_transient/1,
    requires_rediscovery/1
]).

%% Malformed messages, resenging them won't help
is_transient({error, {encode_msg, _}}) ->
    false;
%% This is the final batch timeout, including all the retries
is_transient({error, ?ERROR_TIMEOUT}) ->
    false;
is_transient({error, _}) ->
    true.

requires_rediscovery({error, {encode_msg, _}}) ->
    false;
requires_rediscovery({error, _}) ->
    true.
