-module(hstreamdb_shard_lookup_bm).

-export([make_shard_infos/1, benchmark/2]).

make_shard_info(From, To) ->
    #{
        startHashRangeKey => integer_to_binary(From),
        endHashRangeKey => integer_to_binary(To),
        epoch => 5,
        isActive => true,
        shardId => erlang:unique_integer([positive]),
        streamName => <<"stream">>
    }.

make_shard_infos(N) ->
    <<MaxHash:128/big-unsigned-integer>> = list_to_binary(
        lists:duplicate(16, 255)
    ),
    Step = MaxHash div N,
    make_shard_infos(N, Step, MaxHash, []).

make_shard_infos(1, _Step, To, Acc) ->
    [make_shard_info(0, To) | Acc];
make_shard_infos(N, Step, To, Acc) when N > 1 ->
    From = To - Step + 1,
    make_shard_infos(N - 1, Step, To - Step, [make_shard_info(From, To) | Acc]).

benchmark(strong_rand_bytes, _N) ->
    timing:function(
        fun() ->
            _ = crypto:strong_rand_bytes(16)
        end
    );
benchmark(find_shard, N) ->
    Shards = hstreamdb_key_mgr:index_shards(make_shard_infos(N)),
    timing:function(
        fun() ->
            PartitioningKey = crypto:strong_rand_bytes(16),
            _ = hstreamdb_key_mgr:find_shard(Shards, PartitioningKey)
        end
    ).
