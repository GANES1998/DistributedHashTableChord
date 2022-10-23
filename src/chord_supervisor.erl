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
      io:format("All nodes spawned ~n"),
      % Return the spawned node mapping
      SpawnedNodesMap;
    true ->
      % Spawn a node for current node number by generating hash with node number.
      %%  CurrentHash = crypto:hash(sha1, CurrentNodeNum),
      % Use a 30 Bit Random number in place of hash
      CurrentHash = rand:uniform(trunc(math:pow(2, ?BITCOUNT))),
      % Spawn and link the new node
      Pid = spawn_link(node(), chord_worker, main, [CurrentHash, self()]),
      %% Spawn the next node
      spawn_chord_nodes(CurrentNodeNum + 1, MaxNodes, orddict:store(CurrentHash, Pid, SpawnedNodesMap))
  end.

%% Send all nodes their predecessor and successor.
send_pred_successor(Iterator, NumNodes, NodesPidList) ->
  if
    Iterator > NumNodes ->
      io:format("Predecessor and Successor send to all nodes~n");
    true ->
      %% Get the index of previous and successor nodes
      PrevIter = util:get_prev_node_index(Iterator, NumNodes),
      SuccIter = util:get_succ_node_index(Iterator, NumNodes),

      io:format("NOT BREAKING ~p~n", [lists:flatlength(NodesPidList)]),
      io:format("NTH ~p", [PrevIter]),
      io:format("~p", [lists:nth(PrevIter, NodesPidList)]),

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

listen_to_workers(CurrentNode, NumNodes, SortedNodesList, NumMessages, Hops) ->
  if
    CurrentNode > NumNodes ->
      io:format("All Nodes Terminate - Stoping server ~p", [CurrentNode]),
      {NumMessages, Hops};
    true ->
      receive
        {send_req, {SenderHash, SenderPid}} ->
          %% Choose Random Node to send the storage request
          {RandomHash, RandomPid} = util:choose_random_node(SortedNodesList),

          %% Assume the hash to be a random number generated in next statement.
          HashToStore = rand:uniform(trunc(math:pow(2, ?BITCOUNT))),

          %% Send to the chosen Random index node
          RandomPid ! {hash_request, {HashToStore, HashToStore, {SenderHash, SenderPid}, 1}},

          %% Log a send
          io:format("[~p] Send Request [Hash = ~p] to [~p](~p)~n", [SenderHash, HashToStore, RandomHash, RandomPid]),

          %% listen again for events from workers
          listen_to_workers(CurrentNode, NumNodes, SortedNodesList, NumMessages, Hops);
        %% A successfully stored message
        {success, {_, _, _, HopCount}} ->
          io:format("Success message with hops [~p]~n", [HopCount]),
          %% Iterate by adding number of messages and hop count
          listen_to_workers(CurrentNode, NumNodes, SortedNodesList, NumMessages + 1, Hops + HopCount);
        {terminate, {Hash, _Pid}} ->
          %% Iterate by adding terminated node
          io:format("Node [~p] terminated~n", [Hash]),

          listen_to_workers(CurrentNode + 1, NumNodes, SortedNodesList, NumMessages, Hops)
        after 20000 ->
          io:format("[~p] out of [~p] (~p%) workers terminated successfully executing [~p] Messages with [~p] Hops~n", [CurrentNode, NumNodes, (CurrentNode / NumNodes  * 100), NumMessages, Hops]),
          {NumMessages, Hops}
      end
  end.

initiate_chord(NumNodes, _NumRequests) ->
  % Register Pid
  register(sup_pid, self()),

  % Spawn the number of nodes.
  NodePidMap = spawn_chord_nodes(1, NumNodes, orddict:new()),
  SortedNodesList = orddict:to_list(NodePidMap),

  %% Sorted Nodes List is

  io:format("Sorted Nodes are [~p]~n", [SortedNodesList]),
  %% Sleep to make sure all nodes are spawned.
  timer:sleep(5000),

  %% Send prev and succ to the nodes as initializing.
  send_pred_successor(1, NumNodes, SortedNodesList),

  %% Iterate and calculate the finger table for each nodes.
  lists:foreach(
    fun (Node) ->

      io:format("Node is ~p~n", [Node]),

      %% Get Hash and Pid for any node
      {NodeHash, NodePid} = Node,

      %% Construct finger table of any node
      FingerTable = util:calculate_finger_table(NodeHash, SortedNodesList, 1, ?BITCOUNT, orddict:new()),

      %% Send Finger table to the node
      NodePid ! {finger_table, FingerTable},

      io:format("Sending Finger Table for [~p] to [~p]~n", [NodeHash, NodePid])
    end
    ,SortedNodesList
  ),

  %% Start listening to workers for statistics and termination.
  {Messages, Hops} = listen_to_workers(1, NumNodes, SortedNodesList, 0, 0),

  %% Converge and exit
  io:format("Average Hops count [~p]", [Hops / Messages]).

