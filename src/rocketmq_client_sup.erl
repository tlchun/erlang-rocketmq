%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:53
%%%-------------------------------------------------------------------
-module(rocketmq_client_sup).

-behaviour(supervisor).
-export([start_link/0, init/1]).

-export([ensure_present/3,ensure_absence/1, find_client/1]).


start_link() ->
  supervisor:start_link({local, rocketmq_client_sup},rocketmq_client_sup,[]).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

ensure_present(ClientId, Hosts, Opts) ->
  ChildSpec = child_spec(ClientId, Hosts, Opts),
  case supervisor:start_child(rocketmq_client_sup,ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, client_not_running}
  end.

ensure_absence(ClientId) ->
  case supervisor:terminate_child(rocketmq_client_sup, ClientId) of
    ok ->
      ok = supervisor:delete_child(rocketmq_client_sup, ClientId);
    {error, not_found} -> ok
  end.

find_client(ClientId) ->
  Children = supervisor:which_children(rocketmq_client_sup),
  case lists:keyfind(ClientId, 1, Children) of
    {ClientId, Client, _, _} when is_pid(Client) ->
      {ok, Client};
    {ClientId, Restarting, _, _} -> {error, Restarting};
    false -> erlang:error({no_such_client, ClientId})
  end.

child_spec(ClientId, Hosts, Opts) ->
  #{id => ClientId,
    start =>
    {rocketmq_client, start_link, [ClientId, Hosts, Opts]},
    restart => transient, type => worker,
    modules => [rocketmq_client]}.
