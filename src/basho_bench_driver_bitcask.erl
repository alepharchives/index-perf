%% -------------------------------------------------------------------
%%
%% basho_bench: Benchmarking Suite
%%
%% Copyright (c) 2009-2010 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_bitcask).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, { file,
                  writer}).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->

    %% check concurrency level and assign one writer
    NoWorkers = basho_bench_config:get(concurrent, 1),
    Writer = case Id of
             NoWorkers -> true;
             _ -> false
             end,
    %% Make sure bitcask is available
    case code:which(bitcask) of
        non_existing ->
            ?FAIL_MSG("~s requires bitcask to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    %% Get the target directory
    Dir = basho_bench_config:get(bitcask_dir, "."),
    Filename = filename:join(Dir, "test.bitcask"),



    %% Get any bitcask flags
    Flags = basho_bench_config:get(bitcask_flags, []),
    case Writer of
    true ->
        case bitcask:open(Filename, [read_write, sync_on_put] ++ Flags) of
        {error, Reason} ->
            ?FAIL_MSG("Failed to open bitcask in ~s: ~p\n", [Filename, Reason]);
        File ->
            {ok, #state { file = File, writer=Writer }}
        end;
    false ->
        case bitcask:open(Filename, Flags) of
        {error, Reason} ->
            ?FAIL_MSG("Failed to open bitcask in ~s: ~p\n", [Filename, Reason]);
        File ->
            {ok, #state { file = File, writer=Writer }}
        end
    end.



run(get, KeyGen, _ValueGen, State) ->
    case bitcask:get(State#state.file, KeyGen()) of
        {ok, _Value} ->
            {ok, State};
        not_found ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
    end;
run(put, KeyGen, ValueGen, #state{writer=Writer} = State) ->
    if Writer ->
        case bitcask:put(State#state.file, KeyGen(), ValueGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
        end;
    true ->
        {ok, State}
    end;
run(delete, KeyGen, _ValueGen, #state{writer=Writer}=State) ->
    if Writer ->
        case bitcask:delete(State#state.file, KeyGen()) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason}
        end;
     true ->
        {ok, State}
    end.


