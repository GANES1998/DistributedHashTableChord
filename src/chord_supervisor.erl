%%%-------------------------------------------------------------------
%%% @author ganesonravichandran
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Oct 2022 12:27 pm
%%%-------------------------------------------------------------------
-module(chord_supervisor).
-author("ganesonravichandran").

%% API
-export([]).

spawn_chord_nodes(CurrentNodeNum, NumNodes, Prev) ->
  % Spawn a node for current node number by generating hash with node number.
  CurrentHash = crypto:hash(sha1, CurrentNodeNum),
  Pid = spawn_link(node(), chord_worker, main, [CurrentHash]),
  % Send Current Node as Succ to Prev Node if prev available.
  case prev of
    ok -> io:format("First Node and hence don't have null.");
    {PrevHash, PrevPid} ->
      % Pass current node as success node to previous node
      PrevPid ! {succ_node, {CurrentHash, Pid}},
      % Pass prev to current node
      Pid ! {prev_node, {PrevHash, PrevPid}}
  end.

initiate_chord(NumNodes, NumRequests) ->
  % Spawn the number of nodes.
  spawn_chord_nodes(1, NumNodes, ok).
