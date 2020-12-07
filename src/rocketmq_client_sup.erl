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

-export([ensure_present/3, ensure_absence/1, find_client/1]).

-vsn("4.2.1").

start_link() ->
  supervisor:start_link({local, rocketmq_client_sup}, rocketmq_client_sup, []).

init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

ensure_present(ClientId, Hosts, Opts) ->
%%  获取子进场规范
  ChildSpec = child_spec(ClientId, Hosts, Opts),
%%  启动rocket mq 客户进程监控进程
  case supervisor:start_child(rocketmq_client_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, already_present} -> {error, client_not_running}
  end.

%% 确认客户不在
ensure_absence(ClientId) ->
%%  通过ClientId 停止 rocketmq_client_sup 监控进程下面的子进程
  case supervisor:terminate_child(rocketmq_client_sup, ClientId) of
%%    终止成功
    ok ->
%%      删除rocketmq_client_sup 进程下面的ClientId 子进程
      ok = supervisor:delete_child(rocketmq_client_sup, ClientId);
    {error, not_found} -> ok
  end.

%% 寻找客户端
find_client(ClientId) ->
%%   查找
  Children = supervisor:which_children(rocketmq_client_sup),
%%
  case lists:keyfind(ClientId, 1, Children) of
%%    查找成功
    {ClientId, Client, _, _} when is_pid(Client) ->
      {ok, Client};
    {ClientId, Restarting, _, _} -> {error, Restarting};
    false -> erlang:error({no_such_client, ClientId})
  end.
%% 定义子进程规范
child_spec(ClientId, Hosts, Opts) ->
  #{id => ClientId,
    start =>
    {rocketmq_client, start_link, [ClientId, Hosts, Opts]},
    restart => transient, type => worker,
    modules => [rocketmq_client]}.

