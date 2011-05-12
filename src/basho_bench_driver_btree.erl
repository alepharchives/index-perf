% Copyright 2010 Cloudant

-module(basho_bench_driver_btree).
-author("bob@cloudant.com").

-export([new/1,
         run/4]).

-include_lib("basho_bench/include/basho_bench.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, { batch_size = 1,
                 btree,
                 writer = false
                  }).

new(Id) ->
    {ok, Fd} = case couch_file:open("foo",[]) of
               {error, enoent} ->
                   couch_file:open("foo",[create]);
               {ok, File} ->
                   {ok, File}
               end,
    {ok, Btree} = couch_btree:open(nil, Fd),
    ChunkSize = basho_bench_config:get(chunk_size, 1279),
    BatchSize = basho_bench_config:get(batch_size, 1),
    NoWorkers = basho_bench_config:get(concurrent, 1),
    Writer = case Id of
             NoWorkers -> true;
             _ -> false
             end,
    Btree1 = couch_btree:set_options(Btree, [{chunk_size, ChunkSize}]),

    {ok, #state { batch_size = BatchSize,
                  btree = Btree1, writer = Writer  }}.



run(get, KeyGen, _ValueGen, State) ->
    #state{btree=Bt} = State,
    Key = KeyGen(),
    case couch_btree:lookup(Bt, [Key]) of
    [{ok, {Key, _Val}}] ->
        {ok, State};
    [not_found] ->
        {ok, State};
    _Error ->
        {error, bad_call, State}
    end;

run(put, KeyGen, ValueGen, State) ->
    #state{btree=Bt, batch_size = BatchSize, writer = Writer} = State,
    if Writer ->
        KeyVals = gen_key_vals(KeyGen, ValueGen, BatchSize, []),
        {ok, NBt} = couch_btree:add_remove(Bt,KeyVals,[]),
        {ok, State#state{btree=NBt}};
    true ->
        {ok, State}
    end;

run(delete, KeyGen, _ValueGen, State) ->
    #state{btree=Bt, writer = Writer} = State,
    if Writer ->
        Key = KeyGen(),
        {ok, NBt} = couch_btree:add_remove(Bt,[],[Key]),
        {ok, State#state{btree=NBt}};
    true ->
        {ok, State}
    end.

gen_key_vals(_KeyGen, _ValueGen, 0, KeyVals) ->
    lists:reverse(KeyVals);

gen_key_vals(KeyGen, ValueGen, N, KeyVals) ->
    Key = KeyGen(),
    Val = ValueGen(),
    gen_key_vals(KeyGen, ValueGen, N-1, [{Key, Val} | KeyVals]).

