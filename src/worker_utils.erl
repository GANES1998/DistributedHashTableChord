%%%-------------------------------------------------------------------
%%% @author ganesonravichandran
%%% @copyright (C) 2022, UFL
%%% @doc
%%%
%%% @end
%%% Created : 17. Oct 2022 12:33 pm
%%%-------------------------------------------------------------------
-module(worker_utils).
-author("ganesonravichandran").

%% API
-export([wait_and_get_prev/0, wait_and_get_succ/0, is_between/3, check_valid_key/3, get_next_node/3, wait_and_get_finger_table/0]).


%% Read the previous node of any worker
wait_and_get_prev() ->
  receive
    {prev_node, {PrevNodeHash, PrevNodePid}}->
      {PrevNodeHash, PrevNodePid}
  end.

%% Read the next node of any worker
wait_and_get_succ() ->
  receive
    {succ_node, {SuccNodeHash, SuccNodePid}} ->
      {SuccNodeHash, SuccNodePid}
  end.

wait_and_get_finger_table() ->
  receive
    {finger_table, FingerTable} ->
      FingerTable
  end.

%% Check if the hash is between in a circular ring.
is_between(CheckHash, StartHash, EndHash) ->
  Result = if
    StartHash > EndHash -> CheckHash =< EndHash;
    true -> CheckHash >= StartHash andalso CheckHash < EndHash
  end,
  Result.

%% Check if the key is possible for current node
check_valid_key(CurrentHash, CheckHash, SuccessorHash ) ->
  is_between(CheckHash, CurrentHash, SuccessorHash).

%% Get Correct Table
get_finger_node([H|[]], _) -> H;
get_finger_node([FirstSortedNode|RemainingNodes], CheckHash) ->
  SecondNode = lists:nth(1, RemainingNodes),
  {_, {FirstNodeHash, _}} = FirstSortedNode,
  {_, {NextNodeHash, _}} = SecondNode,

  IsBetween = is_between(CheckHash, FirstNodeHash, NextNodeHash),

  if
    IsBetween -> FirstSortedNode;
    true -> get_finger_node(RemainingNodes, CheckHash)
  end.

%% Get the next node to hop
get_next_node(_CurrentHash, CheckHash, FingerTable) ->
  SortedFingerNodes = orddict:to_list(FingerTable),
  {_, NextNode} = get_finger_node(SortedFingerNodes, CheckHash),
  NextNode.


