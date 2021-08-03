%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:52
%%%-------------------------------------------------------------------
-module(rocketmq_sup).

-behaviour(supervisor).
-export([start_link/0, init/1]).


start_link() ->
  supervisor:start_link({local, rocketmq_sup}, rocketmq_sup, []).

init([]) ->
  %% one_for_all 如果一个子进程停止，所有其他子进程也停止，然后所有进程重启
  %% 如果在intensity秒内重启超过period次,则监督进程会停止所有子进程及其自身
  SupFlags = #{strategy => one_for_all, intensity => 10,period => 5},
  %% 构建监控进程列表
  Children = [client_sup(), producers_sup()],
  %% 要启动的进程列表
  {ok, {SupFlags, Children}}.

client_sup() ->
  #{id => rocketmq_client_sup,
    start => {rocketmq_client_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [rocketmq_client_sup]}.

producers_sup() ->
  #{id => rocketmq_producers_sup,
    start => {rocketmq_producers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor,
    modules => [rocketmq_producers_sup]}.



