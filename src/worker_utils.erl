%%%-------------------------------------------------------------------
%%% @author ganesonravichandran
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Oct 2022 12:33 pm
%%%-------------------------------------------------------------------
-module(worker_utils).
-author("ganesonravichandran").

%% API
-export([wait_and_get_prev/0, wait_and_get_succ/0, is_between/3]).


wait_and_get_prev() ->
  receive
    {prev_node, {PrevNodeHash, PrevNodePid}}-> {PrevNodeHash, PrevNodePid}
  end.

wait_and_get_succ() ->
  receive
    {succ_node, {SuccNodeHash, SuccNodePid}} -> {SuccNodeHash, SuccNodePid}
  end.

is_between(CheckHash, StartHash, EndHash) ->
  CheckHash > StartHash andalso CheckHash =< EndHash.
