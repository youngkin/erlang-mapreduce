% Copyright (c) 2011-2012, Tom Van Cutsem, Vrije Universiteit Brussel
% All rights reserved.
%
% Redistribution and use in source and binary forms, with or without
% modification, are permitted provided that the following conditions are met:
%    * Redistributions of source code must retain the above copyright
%      notice, this list of conditions and the following disclaimer.
%    * Redistributions in binary form must reproduce the above copyright
%      notice, this list of conditions and the following disclaimer in the
%      documentation and/or other materials provided with the distribution.
%    * Neither the name of the Vrije Universiteit Brussel nor the
%      names of its contributors may be used to endorse or promote products
%      derived from this software without specific prior written permission.
%
%THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
%ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
%WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%DISCLAIMED. IN NO EVENT SHALL VRIJE UNIVERSITEIT BRUSSEL BE LIABLE FOR ANY
%DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
%(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES
%LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
%ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
%SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

% A basic parallel (but not distributed) implementation of the Google
% MapReduce pattern.

%% NOTE: module definition
-module(mapreduce).

%% NOTE: public (exported) functions
-export([mapreduce/3]).

%% NOTE: import lists:foreach/2, can be referenced as a local function
%% NOTE: foreach/2 is a function (foreach) that takes 2 arguments (arity)
%% NOTE: function name and arity uniquely define a function
-import(lists, [foreach/2]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Starts the MapReduce process. It is a general implementation that is
%%  provided with the input source, the mapping and reduce functions that will
%%  be applied to the input source.
%%
%%  The MapReduce process is started in it's own process which is a child of
%%  this process. This process will block waiting for the child mapreduce 
%%  process to return it's results.
%%
%%  Parameters
%%      - Input 
%%          An arbitrary list of Key/Value tuples [{K1, V1}]. In the context of 
%%          demo_inverted_index, K1 is an index and V1 is a file name. E.g., 
%%          [{1, test/cats}, {2, test/dogs}, {3, test/cars}]
%%
%%      - Map(K1, V1, Emit)
%%          * A function that takes an arbitrary {K1, V1} tuple. In the context
%%            of demo_inverted_index K1 is an index that identifies a file and V1 
%%            is the associated file name. 
%%          * Emit is a callback function from this module (collect_replies) that 
%%            takes a tuple of {K2, V2} to be captured as an intermediate result.
%%            In the context of demo_inverted_index K2 is a word contained within 
%%            the file and V2 is the name of the associated file.
%%
%%      - Reduce(K2, List[V2], Emit)
%%          * A function that takes an arbitrary K2 and an associated list of 
%%            V2 variables. In the context of demo_inverted_index K2 is a word
%%            and V2 is a list of the files that contain that word.
%%          * Emit is the same callback function described above for Map.
%%        In the context of demo_inverted_index the reduce function doesn't have
%%        much of an effect given the use of a Dictionary to store the intermediate
%%        results. The dictionary pretty effectively contains the final 
%%        representation of the map/reduce process. If a List would have been
%%        used for the intermediate results then Reduce could have collapsed the
%%        multiple list entries for a word contained in multiple files into a
%%        single tuple. 
%%
%%        For example: 
%%        [{jaguar,test/cars}, {jaguar, test/cats}] => {jaguar, [test/cars, test/cats]}
%%
%% Returns a Map[K2,List[V2]]
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
mapreduce(Input, Map, Reduce) ->
  S = self(),
%% NOTE: creates (spawns) a new process
%% NOTE: The new process runs the anonymous function provided as an argument.
%% NOTE: In this case the fun calls the local master/4 function
  Pid = spawn(fun() -> master(S, Map, Reduce, Input) end),

%% NOTE: This is a receive loop.
%% NOTE: It waits for the Pid from the spawned function above to send it a message
%% NOTE: {Pid, Result} ensures that it will receive a message from the process who's
%% NOTE: process ID == Pid. It will bind the second value received to the variable
%% NOTE: "Result"
  receive
    {Pid, Result} -> 
        io:format("\n\n5. Sending results to client process\n",[]),
        Result
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  Helper function that spawns the worker processes for the map/reduce 
%%  operation.
%%
%%  Parameters:
%%      - MasterPid
%%          * The PID (process ID) of the process to notify when the associated
%%            operation (i.e., map or reduce) has completed.
%%          * MasterPid refers to the process that called the mapreduce function.
%%
%%      - Fun
%%          * The callback function to be invoked on the process referenced by
%%            MasterPid
%%          * In this context Fun refers to the collect_replies function
%%
%%      - Pairs
%%          * The parameters to be provided to MasterPid:Fun
%%          * In this context Pairs refers to the List of tuples that contain
%%            the intermediate or final results.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
spawn_workers(MasterPid, Fun, Pairs) ->
%% NOTE: For every value in "Pairs", spawn the anonymous function "fun" and 
%% NOTE: "link" to it to be notified when it EXITs.
  foreach(fun({K,V}) ->
            spawn_link(fun() -> worker(MasterPid, Fun, {K,V}) end)
          end, Pairs).
  
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  This is the master function that orchestrates the map reduce operation.
%%
%%  It spawns the map workers and collects their results. Then it spawns the
%%  reduce workers and collects their results. Lastly, it returns the results
%%  ultimately to it's parent process.
%%
%%  NOTE: Each of the map and reduce processes operate in parallel without any
%%  shared state. The process running this function will process the output
%%  from each worker until it has handled the results for all workers participating
%%  in a given operation (e.g., the Map processes).
%%
%%  Parameters:
%%      - Parent - the PID of the parent process to return results to
%%      - Map - the mapping function
%%      - Reduce - the reduce function
%%      - Input - the source input for the map/reduce operation
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
master(Parent, Map, Reduce, Input) ->
  process_flag(trap_exit, true), 
  MasterPid = self(),
  
  %% Create the mapper processes, one for each element in Input
  io:format("\n2. Start Mapping processes\n",[]),
  spawn_workers(MasterPid, Map, Input),
          
  M = length(Input),
  %% Wait for M Map processes to terminate
  io:format("    a. Waiting for <<~p>> replies from the Mapping processes\n",[M]),
  Intermediate = collect_replies(M, dict:new()), 
  
  %% Create the reducer processes, one for each intermediate Key
  io:format("\n\n3. Start Reduce processes\n",[]),
  spawn_workers(MasterPid, Reduce, dict:to_list(Intermediate)),
  
  R = dict:size(Intermediate),
  %% Wait for R Reduce processes to terminate
  io:format("    a. Waiting for <<~p>> replies from the Reduce processes\n",[R]),
  Output = collect_replies(R, dict:new()),
  io:format("\n4. Sending results to Master process\n",[]),
%% NOTE: Sends results to Parent (PID) by sending a message
%% NOTE: This will be received by the "Parent"'s receive loop by
%% NOTE: matching on the tuple {Pid, Output} where Pid must match
%% NOTE: the value returned by the spawn call that created this 
%% NOTE: (i.e., self()).
  Parent ! {self(), Output}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  This is the worker process that runs the map and reduce operations. One 
%%  will be created for every map and every reduce process.
%%
%%  Parameters:
%%      - MasterPid - the PID of the process to send the results to.
%%      - Fun - The function to be run for the Map or Reduce process.
%%      - {K2, V2} - The tuple representing the pair of values to be
%%        provided to the Map or Reduce functions.
%%
%%  Here's a description of the call to Fun(K, V, ...) below:
%%      Pass Fun (i.e., map or reduce function), the Key and value that are to
%%      be used by Fun, and an anonymous function that will be used by Fun to
%%      provide the results to the MasterPid (i.e, the collect_replies function).
%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%	Worker must send {K2, V2} messsages to master and then terminate
worker(MasterPid, Fun, {K,V}) ->
  Fun(K, V, fun(K2,V2) -> MasterPid ! {K2, V2} end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  This is the callback function that collects the results from map and reduce
%%  processes.  It runs in a "receive" loop waiting for a result from every
%%  map and every reduce worker process.
%%
%%  Parameters:
%%      - N - the number of outstanding worker processes to wait for replies from.
%%      - Dict - The state of the results received so far. As the name implies,
%%        it is a Dictionary.
%%
%%  The receive loop matches 2 patterns:
%%      - {Key, Val} - The results from a worker process
%%      - {'EXIT', _Who, _Why} - Captures that a worker process has exited. This
%%        will result in waiting for replies from 1 less worker (i.e., the worker
%%        that exited).
%%
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% collect and merge {Key, Value} messages from N processes.
%% When N processes have terminated return a dictionary of {Key, [Value]} pairs
%% NOTE: Tail-recursive compound function definition (i.e., 2 function clauses).
%% NOTE: The first function clause is the halting condition (i.e., stop when the
%% NOTE: value of the first parameter equals 0).
%% NOTE: The second function clause is the tail-recursive function that implements
%% NOTE: the "receive" loop.
collect_replies(0, Dict) -> Dict;
collect_replies(N, Dict) ->
  receive
    {Key, Val} ->
      Dict1 = dict:append(Key, Val, Dict),
      collect_replies(N, Dict1);
    {'EXIT', _Who, _Why} ->
      io:format("    !!! Map/Reduce process exited !!!\n",[]),
      collect_replies(N-1, Dict)
  end.