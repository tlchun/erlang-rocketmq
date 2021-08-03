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

%% 启动进程
start_link() ->
  supervisor:start_link({local, rocketmq_client_sup}, rocketmq_client_sup, []).
%% 进程初始化
init([]) ->
  %% 监控策略
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
  %% 子进程列表
  Children = [],
  {ok, {SupFlags, Children}}.

%% 确保进程启动
ensure_present(ClientId, Hosts, Opts) ->
  %% 进程规范定义
  ChildSpec = child_spec(ClientId, Hosts, Opts),
  %% 启动 rocketmq_client_sup 监控进程---子进程rocketmq_client
  case supervisor:start_child(rocketmq_client_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};  %% 启动成功
    {error, {already_started, Pid}} -> {ok, Pid};  %% 已经启动成功
    {error, already_present} -> {error, client_not_running}  %% 启动失败
  end.

%% 确保进程不存在
ensure_absence(ClientId) ->
  %% 终止 rocketmq_client_sup 的子进程 ClientId
  case supervisor:terminate_child(rocketmq_client_sup, ClientId) of
    ok ->
      %% 终止成功后删除改进程
      ok = supervisor:delete_child(rocketmq_client_sup, ClientId);
    %% 没有发现
    {error, not_found} -> ok
  end.
%% 通过 ClientId 查询 客户端
find_client(ClientId) ->
  %% 获取 rocketmq_client_sup 进程下有多少个子进程和他的状态
  Children = supervisor:which_children(rocketmq_client_sup),
  %% 通过 ClientId 从 Children 查找
  case lists:keyfind(ClientId, 1, Children) of
    %% 匹配出一个客户端
    {ClientId, Client, _, _} when is_pid(Client) -> {ok, Client};
    %% 客户端正在启动
    {ClientId, Restarting, _, _} -> {error, Restarting};
    %% 没匹配到
    false -> erlang:error({no_such_client, ClientId})
  end.

child_spec(ClientId, Hosts, Opts) ->
  #{id => ClientId,
    start => {rocketmq_client, start_link, [ClientId, Hosts, Opts]},
    restart => transient, type => worker,
    modules => [rocketmq_client]}.


