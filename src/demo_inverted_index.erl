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

% Example of building an inverted index of a set of text documents

%% NOTE: module definition
-module(demo_inverted_index).

%% NOTE: public functions
-export([index/1, query_index/2, list_numbered_files/1]).

%% NOTE: provide a local name for a function in another module
-import(mapreduce, [mapreduce/3]).

%% Auxiliary function to generate {Index, FileName} input
list_numbered_files(DirName) ->
  {ok, Files} = file:list_dir(DirName),
  FullFiles = [ filename:join(DirName, File) || File <- Files ],
  Indices = lists:seq(1, length(Files)),
  lists:zip(Indices, FullFiles). % {Index, FileName} tuples

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Inverse Index
%%
%%  Starts process by:
%%      1.  Getting the input files
%%      2.  Calling "mapreduce" passing the input files and the Map & Reduce 
%%          functions
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
index(DirName) ->
  io:format("1. Start map/reduce process for files located in directory: ~p\n", 
            [DirName]),
  NumberedFiles = list_numbered_files(DirName),
  mapreduce(NumberedFiles, fun find_words/3, fun remove_duplicates/3).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  The mapping function which reads the words contained in a file and puts them
%%  into a list.
%%
%%  For each word in the Words list:
%%      1.  Calls the Emit function passing the current Word and associated file
%%          name
%%          a.  Emit is the function that collects intermediate results
%%          b.  _Index is a named, but unused value
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
find_words(_Index, FileName, Emit) ->
  io:format("       Map worker for file: ~p\n", 
            [FileName]),
  {ok, [Words]} = file:consult(FileName),
  lists:foreach(fun (Word) -> Emit(Word, FileName) end, Words).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%%  The reduce function which associates a given word with all the files that 
%%  contain that word.
%%
%%  For each file in the Files list (after removing any duplicates):
%%      1.  Calls the Emit function passing the current Word and associated file
%%          name
%%          a.  Emit is the function that collects intermediate results
%%          b.  _Index is a named, but unused value
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
remove_duplicates(Word, FileNames, Emit) ->
  io:format("       Reduce worker for Word <<~p>> and associated FileNames <<~p>>\n", 
            [Word, FileNames]),
  UniqueFiles = sets:to_list(sets:from_list(FileNames)),
  lists:foreach(fun (FileName) -> Emit(Word, FileName) end, UniqueFiles).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Query the index after the map/reduce operation is complete.
%%
%%  Given an Index (i.e., a Map or Dictionary) and a Word, return the files
%%  that contained that word.
%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
query_index(Index, Word) ->
  dict:find(Word, Index).