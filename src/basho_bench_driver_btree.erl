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

writer(Bt0) ->
    receive
        {From, {put, KVs}} ->
            {ok, Bt1} = couch_btree:add_remove(Bt0, KVs, []),
            From ! {ok, Bt1},
            writer(Bt1);
        {From, {delete, Key}} ->
            {ok, Bt1} = couch_btree:add_remove(Bt0, [], [Key]),
            From ! {ok, Bt1},
            writer(Bt1)
    end.

new(Id) ->
    ChunkSize = basho_bench_config:get(chunk_size, 1279),
    BatchSize = basho_bench_config:get(batch_size, 1),

    {ok, Db} = erleveldb:open_db("foo", [create_if_missing]),

    % {ok, Fd} = case couch_file:open("foo",[]) of
    %     {error, enoent} ->
    %         couch_file:open("foo",[create]);
    %     {ok, File} ->
    %         {ok, File}
    % end,

    {ok, Btree} = couch_btree:open(nil, Db),
    Btree1 = couch_btree:set_options(Btree, [{chunk_size, ChunkSize}]),
    
    Writer = case {whereis(btree_writer), Id} of
        {Pid, _} when is_pid(Pid) ->
            Pid;
        {_, 1} ->
            Pid = spawn(fun() -> writer(Btree) end),
            register(btree_writer, Pid),
            Pid;
        _ ->
            get_writer(9)
    end,

    {ok, #state { batch_size = BatchSize,
                  btree = Btree1, writer = Writer  }}.

get_writer(0) ->
    exit(no_btree_writer_found);
get_writer(N) ->
    case whereis(btree_writer) of
        Pid when is_pid(Pid) ->
            Pid;
        _ ->
            timer:sleep(333),
            get_writer(N-1)
    end.

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
    #state{batch_size = BatchSize, writer = Writer} = State,
    KeyVals = gen_key_vals(KeyGen, ValueGen, BatchSize, []),
    Writer ! {self(), {put, KeyVals}},
    receive
        {ok, NBt} ->
            {ok, State#state{btree=NBt}}
    after 5000 ->
        exit(write_timeout)
    end;

run(delete, KeyGen, _ValueGen, State) ->
    #state{writer = Writer} = State,
    Writer ! {self(), {delete, KeyGen()}},
    receive
        {ok, NBt} ->
            {ok, State#state{btree=NBt}}
    after 5000 ->
        exit(write_timeout)
    end.

gen_key_vals(_KeyGen, _ValueGen, 0, KeyVals) ->
    lists:reverse(KeyVals);

gen_key_vals(KeyGen, ValueGen, N, KeyVals) ->
    Key = KeyGen(),
    Val = ValueGen(),
    gen_key_vals(KeyGen, ValueGen, N-1, [{Key, Val} | KeyVals]).

