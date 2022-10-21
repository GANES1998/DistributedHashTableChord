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
-define(BITCOUNT, 32).

%% API
-export([spawn_chord_nodes/3, initiate_chord/2, send_pred_successor/3]).

spawn_chord_nodes(CurrentNodeNum, MaxNodes, SpawnedNodesMap) ->
  if
    CurrentNodeNum > MaxNodes ->
      io:format("All nodes spawned ~n as ~p", [SpawnedNodesMap]),
      % Return the spawned node mapping
      SpawnedNodesMap;
    true ->
      % Spawn a node for current node number by generating hash with node number.
      %%  CurrentHash = crypto:hash(sha1, CurrentNodeNum),
      % Use a 30 Bit Random number in place of hash
      CurrentHash = rand:uniform(trunc(math:pow(2, 16))),
      % Spawn and link the new node
      Pid = spawn_link(node(), chord_worker, main, [CurrentHash]),
      %% Spawn the next node
      spawn_chord_nodes(CurrentNodeNum + 1, MaxNodes, orddict:store(CurrentHash, Pid, SpawnedNodesMap))
  end.

%% Send all nodes their predecessor and successor.
send_pred_successor(Iterator, NumNodes, NodesPidList) ->
  if
    Iterator > NumNodes ->
      io:format("Predecessor and Successor send to all nodes");
    true ->
      %% Get the index of previous and successor nodes
      PrevIter = util:get_prev_node_index(Iterator, NumNodes),
      SuccIter = util:get_succ_node_index(Iterator, NumNodes),

      %% Get the hash and pid of the previous and successor nodes
      {PrevHash, PrevPid} = lists:nth(PrevIter, NodesPidList),
      {SuccHash, SuccPid} = lists:nth(SuccIter, NodesPidList),

      %% Send prev hash and successor hash and pid
      {_, CurrentPid} = lists:nth(Iterator, NodesPidList),
      CurrentPid ! {prev_node, {PrevHash, PrevPid}},
      CurrentPid ! {succ_node, {SuccHash, SuccPid}},

      %% Iterate for the next node
      send_pred_successor(Iterator + 1, NumNodes, NodesPidList)
  end.

initiate_chord(NumNodes, _NumRequests) ->
  % Spawn the number of nodes.
  NodePidMap = spawn_chord_nodes(1, NumNodes, orddict:new()),

  %% Send prev and succ to the nodes as initializing.
  send_pred_successor(1, NumNodes, orddict:to_list(NodePidMap)),

  %% Wait indefinitely
  receive
    a -> io:format('')
  end.
