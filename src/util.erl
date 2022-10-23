%%%-------------------------------------------------------------------
%%% @author ganesonravichandran
%%% @copyright (C) 2022, UFL
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 6:05 pm
%%%-------------------------------------------------------------------
-module(util).
-author("ganesonravichandran").

%% API
-export([get_prev_node_index/2, get_succ_node_index/2, calculate_finger_table/5, choose_random_node/1]).

get_prev_node_index(Index, NumNodes) ->
  case Index of
    1 -> NumNodes;
    _ -> Index - 1
  end.

get_succ_node_index(Index, NumNodes) ->
  case Index of
    NumNodes -> 1;
    _ -> Index + 1
  end.

get_kth_finger_position(StartPosition, K, Bits) ->

  K_th_position = trunc(math:pow(2, K - 1)),

  NewPosition = StartPosition + K_th_position,

  MaxNode = trunc(math:pow(2, Bits)) -1,

  if
    NewPosition > MaxNode -> (NewPosition - MaxNode);
    true -> NewPosition
  end.

get_node_first_node_greater_than(Nodes, CheckNodeHash) ->
  MoreHashNodes = lists:filter(
    fun({Hash, _Pid}) -> Hash > CheckNodeHash end
    ,
    Nodes
  ),

  MoreHashNodesCount = lists:flatlength(MoreHashNodes),

  if
    MoreHashNodesCount > 0 -> lists:nth(1, MoreHashNodes);
    true -> lists:nth(1, Nodes)
  end.

calculate_finger_table(StartNode, SortedNodesList, CurrentK, Bits, FingerTableMap) ->

  if
    CurrentK > Bits ->
%%      io:format("All Finger Table [~p] calculated for Start Node [ ~p ]", [FingerTableMap, StartNode]),
      FingerTableMap;
    true ->
      %% Calculate the expected hash at the entry --
      K_th_Node_Expected_Hash = get_kth_finger_position(StartNode, CurrentK, Bits),

%%      io:format("~p th node expected hash ~p for current hash [~p]", [CurrentK, K_th_Node_Expected_Hash, StartNode]),

      %% Calculate the kth node element (Actual element)
      K_th_Node_Element = get_node_first_node_greater_than(SortedNodesList, K_th_Node_Expected_Hash),

      %% Add the element to kth location in the current nodes finger table and iterate for k+1^{th} position
      calculate_finger_table(StartNode, SortedNodesList, CurrentK + 1, Bits, orddict:store(CurrentK, K_th_Node_Element, FingerTableMap))

  end.

choose_random_node(SortedNodesList) ->
  Len = lists:flatlength(SortedNodesList),

  RanIndex = rand:uniform(Len),

  {Hash, Pid} = lists:nth(RanIndex, SortedNodesList),

  {Hash, Pid}.