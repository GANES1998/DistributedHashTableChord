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
-define(MAX_HOPS, 20).
-define(sup_pid, sup_pid).

%% API
-export([main/2, main_loop/4, stop_timer/2]).

%%% Wait for the initial time to get the hash and pid of previous and next nodes.
wait_and_get_adj() ->
  {PrevHash, PrevPid} = worker_utils:wait_and_get_prev(),
  {SuccHash, SuccPid} = worker_utils:wait_and_get_succ(),
  {{PrevHash, PrevPid}, {SuccHash, SuccPid}}.
%%  {SuccHash, SuccPid}.

%% Send stabile message to same worker at regular intervals
initiate_self_stabilization() ->
  timer:send_interval(?STABILE_INTERVAL, chord_worker, stabilize).

%% handle_predecessor_request
handle_pred_request(CallerPid, CurrentHash) ->
  %% Reply to the caller with Current Hash
  CallerPid ! {CurrentHash, self()}.

%% Handle predecessor notifications, When a pred sends that it could be a pred
handle_pred_notification(CurrentPred, CurrentHash, {NewPredHash, NewPredPid}, Successor, FingerTable) ->

  %% If there is no current predecessor, then set it as current pred
  case CurrentPred of
    %% If there is no pred, set new Pred as current pred
    none -> main_loop(CurrentHash, {NewPredPid, NewPredHash}, Successor, FingerTable);
    %% There exist a CurrentPred
    {CurrentPredHash, _} ->
      %% If the new pred is between CurrentPred and Current Hash
      IsBetween = worker_utils:is_between(NewPredHash, CurrentHash, CurrentPredHash),
      if
        IsBetween ->
          %% If the new pred is between CurrentPred and Current Hash, Update the pred to incoming pred
          main_loop(CurrentHash, {NewPredHash, NewPredPid}, Successor, FingerTable);
        true ->
          %% Do nothing, loop on
          main_loop(CurrentHash, CurrentPred, Successor, FingerTable)
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
        {CurrentHash, _} -> done;
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

%% Method that will trigger Random request some node in the network.
initialize_trigger_request() ->
  {ok, TimerRef} = timer:send_interval(1000, send_storage_request),
  TimerRef.

stop_timer(TimerRef, CurrentHash) ->
  io:format("Stopping Timer for Node [~p] ~n", [CurrentHash]),
  timer:cancel(TimerRef).

main_loop(CurrentHash, Pred, Successor, FingerTable) ->
  {SuccessorHash, SuccessorPid} = Successor,
  receive
  %% Stabilise the predecessor and successor.
    stabilize ->
      io:format("Stabilize called"),

      %% Stabilize will internally make call to main loop
      stabilize(Pred, CurrentHash, {SuccessorPid, SuccessorHash});

  %% Send storage request
    send_storage_request ->
      sup_pid ! {send_req, {CurrentHash, self()}},

      %% Again listen for messages
      main_loop(CurrentHash, Pred, Successor, FingerTable);

  %% Message to store a new input -> Nothing But a request for hash table. If stored successfully send to the caller.
    {hash_request, {Hash, Message, {CallerHash, CallerPid}, HopCount}} ->
      if
        HopCount > ?MAX_HOPS ->
          io:format("Message Hash [~p] reached Max Hop Count while reaching [~p]. So, dropping..~n", [Hash, CurrentHash]);
        true ->
%%          io:format("Hash Request received for [~p] at [~p]~n", [Hash, CurrentHash]),

          IsKeyValidForCurrentNode = worker_utils:check_valid_key(CurrentHash , Hash, SuccessorHash),
          if
            IsKeyValidForCurrentNode ->
              io:format("Hash [~p] Successfully Stored in Node [~p] with Successor [~p] at Hop no: [~p]~n", [Hash, CurrentHash, SuccessorHash, HopCount]),
              %% Assuming this node will store into its data store, we will intimate the original Caller of this request.
              CallerPid ! {success_stored, {Hash, Message, {CurrentHash, self()}, HopCount + 1}},

              main_loop(CurrentHash, Pred, Successor, FingerTable);
            true ->

              % Check which next node to contact.
              {NextHash, NextPid} = worker_utils:get_next_node(CurrentHash, Hash, FingerTable),

              io:format("Node [~p] received to search for hash [~p]. But it forwarded to [~p]~n", [CurrentHash, Hash, NextHash]),

              % Check to the next node in Finger table for the same message.
              NextPid ! {hash_request, {Hash, Message, {CallerHash, CallerPid}, HopCount + 1}},

              main_loop(CurrentHash, Pred, Successor, FingerTable)
          end
      end;


  %% Caller Successfully Stored a request.
    {success_stored, {Hash, Message, {StoredNodeHash, _StoredNodePid}, HopCount}} ->
      io:format("Success stored Hash [~p] send by me [~p] on [~p]~n", [Hash, CurrentHash, StoredNodeHash]),

%% Send statistics information to the supervisor for calculating the average hop node.
      sup_pid ! {success, {Hash, Message, StoredNodeHash, HopCount}},

%% Listen again
      main_loop(CurrentHash, Pred, Successor, FingerTable);

  %% Finger Table, receive updated finger table
    {finger_table, NewFingerTable} ->
      io:format("Finger Table Calculated for node [ ~p ] is  [~p]~n", [CurrentHash, orddict:to_list(NewFingerTable)]),
      main_loop(CurrentHash, Pred, Successor, NewFingerTable);

%% When a prospective predecessor requests to check if it could be a predecessor.
    {notify_could_be_pred, {PredHash, PredPid}} ->
      handle_pred_notification(Pred, CurrentHash, {PredHash, PredPid}, Successor, FingerTable);

%% When predecessor requests to know successor (current node's) predecessor.
    {request_pred, {_CallerHash, CallerPid}} ->

%% Reply to caller node with Predecessor.
      handle_pred_request(CallerPid, CurrentHash),

      main_loop(CurrentHash, Pred, Successor, FingerTable);

    %% You
    _ -> io:format("RISK - RANDOM MESSAGE APPEARED")
  after 20000 ->
    io:format("Node [~p] is idle for 20000 seconds. So, terminating~n", [CurrentHash]),

    %% Send terminate id to the help,
    sup_pid ! {terminate, {CurrentHash, self()}}
  end.

main(CurrentHash, NumRequest) ->

  %% Console print that a new node is initialized
  io:format("~p Node started and it is waiting for successor~n", [CurrentHash]),

  %% Get the successor hash and successor PID from supervisor.
  {{PredecessorHash, PredecessorPid}, {SuccessorHash, SuccessorPid}} = wait_and_get_adj(),

  io:format("Hash [ ~p, ~p] obtained pev = [ ~p, ~p ] and succ = [~p, ~p]~n",
    [CurrentHash, self(),
      PredecessorHash, PredecessorPid,
      SuccessorHash, SuccessorPid]),

%%   Schedule stabilize to self in timer.
%%  initiate_self_stabilization(),

  %% Get Initial Finger Table
  FingerTable = worker_utils:wait_and_get_finger_table(),
  io:format("Finger Table Calculated for node [ ~p ] is  [~p]~n", [CurrentHash, orddict:to_list(FingerTable)]),

  %% Initiate sending storage request to random nodes.
  TimerRef = initialize_trigger_request(),

  %% KillTimer After Some Seconds.
  timer:apply_after(NumRequest * 1000, ?MODULE, stop_timer, [TimerRef, CurrentHash]),

%%   Got To Main Loop
  main_loop(CurrentHash, {PredecessorHash, PredecessorPid}, {SuccessorHash, SuccessorPid}, FingerTable).



