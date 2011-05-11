% Copyright 2010 Cloudant

-module(basho_bench_driver_erleveldb).
-author("bob@cloudant.com").

-export([new/1,
         run/4]).

-include_lib("basho_bench/include/basho_bench.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, { db,
                 writer
                  }).

new(Id) ->
    {ok, Db} = erleveldb:open_db("foo",[create_if_missing]),
    NoWorkers = basho_bench_config:get(concurrent, 1),
    Writer = case Id of
             NoWorkers -> true;
             _ -> false
             end,
    {ok, #state { db = Db, writer = Writer  }}.

run(get, KeyGen, _ValueGen, State) ->
    #state{db=Db} = State,
    Key = KeyGen(),
    case erleveldb:get(Db, Key) of
    {ok, _Val} ->
        {ok, State};
    {error, not_found} ->
        {ok, State};
    _Error ->
        {error, bad_call, State}
    end;

run(put, KeyGen, ValueGen, State) ->
    #state{db=Db, writer = Writer} = State,
    if Writer ->
        Key = KeyGen(),
        Val = ValueGen(),
        erleveldb:put(Db,Key,Val),
        {ok, State};
    true ->
        {ok, State}
    end;

run(delete, KeyGen, _ValueGen, State) ->
    #state{db=Db, writer = Writer} = State,
    if Writer ->
        Key = KeyGen(),
        erleveldb:del(Db,Key),
        {ok, State};
    true ->
        {ok, State}
    end.
