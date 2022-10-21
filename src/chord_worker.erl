%%%-------------------------------------------------------------------
%%% @author ganesonravichandran
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Oct 2022 12:29 pm
%%%-------------------------------------------------------------------
-module(chord_worker).
-author("ganesonravichandran").
-define(STABILE_INTERVAL, 100).

%% API
-export([main/1]).

%%% Wait for the initial time to get the hash and pid of previous and next nodes.
wait_and_get_adj() ->
  {PrevHash, PrevPid} = worker_utils:wait_and_get_prev(),
  {SuccHash, SuccPid} = worker_utils:wait_and_get_succ(),
  {{PrevHash, PrevPid}, {SuccHash, SuccPid}}.
%%  {SuccHash, SuccPid}.

%% Send stabile message to same worker at regular intervals
initiate_self_stabilization() ->
  timer:apply_interval(?STABILE_INTERVAL, chord_worker, stabilize).

%% handle_predecessor_request
handle_pred_request(CallerPid, CurrentPid) ->
  CallerPid ! {CurrentPid, self()}.

%% Stabilize Method, To Fix the successor.
stabilize({PredHash, PredPid}, CurrentHash, {SuccessorHash, SuccPid}) ->
  %% Send request for pred to current successor
  SuccPid ! {request_pred, {CurrentHash, self()}},

  %% Receive from Successor, for pred value
  receive
    {pred_success, Pred} ->
      case Pred of
        %% If predecessor of success is same, then no need to update predecssors
        {CurrentHash, _} ->  done;
        %% No pred, when a node joins
        nil ->
          %% Notify about existence -
          done;
        {SuccessorHash, _} ->
          %% The predecessor is same, it was the single node in the network, so, notify of the presence of pred (current node)
          done;
        {NewPredHash, NewPredPid} ->
          %% Check if this node is between Current Pred and Current Succ:
          IsBetween = worker_utils:is_between(NewPredHash, PredHash, SuccessorHash),
          if
              %% Slide the current node between both the nodes. Update the predecessor.
             IsBetween ->
                  stabilize({PredHash, PredPid}, CurrentHash, {NewPredHash, NewPredPid});
              %% Add the current node's successor as predeccessor
             true -> done
          end
      end
  end.

main(CurrentHash) ->
  %% Console print that a new node is initialized
  io:format("~p Node started and it is waiting for successor"),

  %% Get the successor hash and successor PID from supervisor.
  {{PredecessorHash, PredecessorPid},{SuccessorHash, SuccessorPid}} = wait_and_get_adj(),

  %% Schedule stabilize to self in timer.



%%  {{PrevHash, PrevPid}, {SuccHash, SuccPid}} = wait_and_get_adj(),
%%  io:format("~p, ~p and ~p, ~p", [PrevHash, PrevPid, SuccHash, SuccPid]).


