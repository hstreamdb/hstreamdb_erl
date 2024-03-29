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

-ifndef(HSTREAMDB_HRL).
-define(HSTREAMDB_HRL, true).

-include("hstreamdb_client.hrl").

-define(HSTREAMDB_GEN_CLIENT, hstream_server_h_stream_api_client).

-define(DEFAULT_HSTREAMDB_PORT, 6570).

-define(DEFAULT_MAX_RECORDS, 100).
-define(DEFAULT_MAX_BATCHES, 500).
-define(DEFAULT_INTERVAL, 3000).
-define(POOL_TIMEOUT, 60000).

-ifdef(TEST).
-define(DEFAULT_WRITER_POOL_SIZE, 4).
-define(DEFAULT_BUFFER_POOL_SIZE, 2).
-else.
-define(DEFAULT_WRITER_POOL_SIZE, 64).
-define(DEFAULT_BUFFER_POOL_SIZE, 8).
-endif.

-define(DEFAULT_BATCH_REAP_TIMEOUT, 120000).
-define(DEFAULT_COMPRESSION, none).

-define(DEFAULT_READER_POOL_SIZE, 16).

-define(DEFAULT_GRACEFUL_STOP_TIMEOUT, 5000).
-define(STOP_TIMEOUT, 1000).
%% seconds
-define(DEAULT_AUTO_RECONNECT, 5).

%% ResourseType enum

-define(RES_STREAM, 0).
-define(RES_SUBSCRIPTION, 1).
-define(RES_SHARD, 2).
-define(RES_SHARD_READER, 3).
-define(RES_CONNECTOR, 4).
-define(RES_QUERY, 5).
-define(RES_VIEW, 6).

-define(DEFAULT_BATCH_BACKOFF_OPTIONS, {100, 1000, 2}).
-define(DEFAULT_BATCH_MAX_RETRIES, 5).

-define(DEFAULT_DISOVERY_BACKOFF_OPTIONS, {100, 5000, 1.5}).

-define(DISCOVERY_TAB, hstreamdb_discovery_tab).

-define(DEFAULT_STREAM_INVALIDATE_TIMEOUT, 60000).

-define(ECHO_MESSAGE, <<"hello">>).

-define(VIA_GPROC(ID), {via, gproc, {n, l, ID}}).

-define(SAFE_CAST_VIA_GPROC(ID, MESSAGE), ?SAFE_CAST_VIA_GPROC(ID, MESSAGE, noproc)).

-define(SAFE_CAST_VIA_GPROC(ID, MESSAGE, NO_PROC_ERROR),
    try gen_statem:cast(?VIA_GPROC(ID), MESSAGE) of
        __RESULT__ -> __RESULT__
    catch
        exit:{noproc, _} -> {error, NO_PROC_ERROR}
    end
).

-define(SAFE_CALL_VIA_GPROC(ID, MESSAGE, TIMEOUT, NO_PROC_ERROR),
    try gen_statem:call(?VIA_GPROC(ID), MESSAGE, TIMEOUT) of
        __RESULT__ -> __RESULT__
    catch
        exit:{noproc, _} -> {error, NO_PROC_ERROR}
    end
).

-record(batch, {
    batch_ref :: reference(),
    shard_id :: integer(),
    tab :: ets:table(),
    req_ref :: reference(),
    compression_type :: hstreamdb_client:compression_type()
}).

-define(MEASURE(LABEL, CODE),
    (fun() ->
        {__TIME__, __RES__} = timer:tc(
            fun() ->
                CODE
            end
        ),
        logger:debug("[hstreamdb] ~p in ~p ms", [LABEL, __TIME__ / 1000]),
        __RES__
    end)()
).

-endif.
