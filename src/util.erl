%%%-------------------------------------------------------------------
%%% @author ganesonravichandran
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 21. Oct 2022 6:05 pm
%%%-------------------------------------------------------------------
-module(util).
-author("ganesonravichandran").

%% API
-export([get_prev_node_index/2, get_succ_node_index/2]).

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

