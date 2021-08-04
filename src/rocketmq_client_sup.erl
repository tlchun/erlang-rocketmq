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

%% 启动客户端监控进程 rocketmq_client_sup
start_link() ->
  supervisor:start_link({local, rocketmq_client_sup}, rocketmq_client_sup, []).

%% 进程启动监控
init([]) ->
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  Children = [],
  {ok, {SupFlags, Children}}.

%% 确保 rocketmq_client_sup 子进程出现
ensure_present(ClientId, Hosts, Opts) ->
  %% 子进程规范
  ChildSpec = child_spec(ClientId, Hosts, Opts),
  %% 启动 rocketmq_client_sup 子进程
  case supervisor:start_child(rocketmq_client_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid}; %% 启动成功，返回进程Pid
    {error, {already_started, Pid}} -> {ok, Pid};%% 已经启动
    {error, already_present} -> {error, client_not_running} %% 没有出现，但是客户端没有运行
  end.
%% 确保客户端离开
ensure_absence(ClientId) ->
  %% 终止 rocketmq_client_sup 监控进程的子进程 ClientId
  case supervisor:terminate_child(rocketmq_client_sup, ClientId) of
    ok ->
      %% 终止成功，然后删除
      ok = supervisor:delete_child(rocketmq_client_sup, ClientId);
    {error, not_found} -> ok
  end.

%% 通过ClientId 找客户端进程ID
find_client(ClientId) ->
  %% 获取 rocketmq_client_sup 子进程列表
  Children = supervisor:which_children(rocketmq_client_sup),
  %%
  case lists:keyfind(ClientId, 1, Children) of
    %% 找到
    {ClientId, Client, _, _} when is_pid(Client) -> {ok, Client};
    %% 正在重启
    {ClientId, Restarting, _, _} -> {error, Restarting};
    %% 没找到
    false -> erlang:error({no_such_client, ClientId})
  end.

%% 子进程规范
child_spec(ClientId, Hosts, Opts) ->
  #{id => ClientId, %% 进程ID
    start => {rocketmq_client, start_link, [ClientId, Hosts, Opts]}, %% M = rocketmq_client, F = start_link ,Args = [ClientId, Hosts, Opts]
    restart => transient, %% 重启类型 如果app以normal的原因终止，没有影响。任何其它终止原因都谁导致整个系统关闭。
    type => worker, %% 进程类型--工作者类型
    modules => [rocketmq_client]}. %% 回调模块名称

