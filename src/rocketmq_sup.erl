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

%% 启动应用根监控者
start_link() ->
  supervisor:start_link({local, rocketmq_sup}, rocketmq_sup, []).

%% 启动回调函数
init([]) ->
  %%  进程规范
  SupFlags = #{strategy => one_for_all, intensity => 10, period => 5},
  %% 客户端监控，生产者监控
  Children = [client_sup(), producers_sup()],
  {ok, {SupFlags, Children}}.

%% 客户端监控者进程启动
client_sup() ->
  #{id => rocketmq_client_sup,
    start => {rocketmq_client_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor, modules => [rocketmq_client_sup]}.

%%生产者监控进程启动
producers_sup() ->
  #{id => rocketmq_producers_sup,
    start => {rocketmq_producers_sup, start_link, []},
    restart => permanent, shutdown => 5000,
    type => supervisor,
    modules => [rocketmq_producers_sup]}.