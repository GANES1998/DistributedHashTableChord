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
  timer:inter(?STABILE_INTERVAL, chord_worker, stabilize).

%% handle_predecessor_request
handle_pred_request(CallerPid, CurrentHash) ->
  %% Reply to the caller with Current Hash
  CallerPid ! {CurrentHash, self()}.

%% Handle predecessor notifications, When a pred sends that it could be a pred
handle_pred_notification(CurrentPred, CurrentHash, {NewPredHash, NewPredPid}, Successor) ->

  %% If there is no current predecessor, then set it as current pred
  case CurrentPred of
    %% If there is no pred, set new Pred as current pred
    none -> main_loop(CurrentHash, {NewPredPid, NewPredHash}, Successor);
    %% There exist a CurrentPred
    {CurrentPredHash, _} ->
      %% If the new pred is between CurrentPred and Current Hash
      IsBetween = worker_utils:is_between(NewPredHash, CurrentHash, CurrentPredHash),
      if
        IsBetween ->
          %% If the new pred is between CurrentPred and Current Hash, Update the pred to incoming pred
          main_loop(CurrentHash, {NewPredHash, NewPredPid}, Successor);
        true ->
          %% Do nothing, loop on
          main_loop(CurrentHash, CurrentPred, Successor)
      end
  end.

%% Stabilize Method, To Fix the successor.
%% TODO - Check and Remove the Current Pred.
stabilize(CurrentPred, CurrentHash, {SuccessorHash, SuccPid}) ->
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
          SuccPid ! {notify_could_be_pred, {CurrentHash, self()}};
        {SuccessorHash, _} ->
          %% The predecessor is same, it was the single node in the network, so, notify of the presence of pred (current node)
          SuccPid ! {notify_could_be_pred, {CurrentHash, self()}};
        {NewPredHash, NewPredPid} ->
          %% Check if this node is between Current Pred and Current Succ:
          IsBetween = worker_utils:is_between(NewPredHash, CurrentHash, SuccessorHash),
          if
              %% Slide the current node between both the nodes. Update the predecessor.
             IsBetween ->
                  stabilize(CurrentPred, CurrentHash, {NewPredHash, NewPredPid});
              %% Add the current node's successor as predeccessor
             true ->
               {notify_could_be_pred, {CurrentHash, self()}}
          end
      end
  end.

main_loop(CurrentHash, Pred, Successor) ->
  {SuccessorPid, SuccessorHash} = Successor,
  receive
    %% Stabilise the predecessor and successor.
    stabilize ->
      stabilize(Pred, CurrentHash, {SuccessorPid, SuccessorHash});

    %% When a prospective predecessor requests to check if it could be a predecessor.
    {notify_could_be_pred, {PredHash, PredPid}} ->
      handle_pred_notification(Pred, CurrentHash, {PredHash, PredPid}, Successor);

    %% When predecessor requests to know successor (current node's) predecessor.
    {request_pred, {_CallerHash, CallerPid}} ->

      %% Reply to caller node with Predecessor.
      handle_pred_request(CallerPid, CurrentHash),

      main_loop(CurrentHash, Pred, Successor)
  end.

main(CurrentHash) ->
  %% Console print that a new node is initialized
  io:format("~p Node started and it is waiting for successor~n", [CurrentHash]),

  %% Get the successor hash and successor PID from supervisor.
  {{PredecessorHash, PredecessorPid},{SuccessorHash, SuccessorPid}} = wait_and_get_adj(),

  io:format("Hash [ ~p, ~p] obtained pev = [ ~p, ~p ] and succ = [~p, ~p]~n",
    [CurrentHash, self(),
    PredecessorHash, PredecessorPid,
    SuccessorHash, SuccessorPid]).

  %% Schedule stabilize to self in timer.
%%  initiate_self_stabilization(),

  %% Got To Main Loop
%%  main_loop(CurrentHash, {PredecessorHash, PredecessorPid}, {SuccessorHash, SuccessorPid}).

%%  {{PrevHash, PrevPid}, {SuccHash, SuccPid}} = wait_and_get_adj(),
%%  io:format("~p, ~p and ~p, ~p", [PrevHash, PrevPid, SuccHash, SuccPid]).


