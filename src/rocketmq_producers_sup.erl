%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:53
%%%-------------------------------------------------------------------
-module(rocketmq_producers_sup).

-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([ensure_present/4, ensure_absence/2]).

%% 启动生产者监控进程
start_link() ->
  supervisor:start_link({local, rocketmq_producers_sup}, rocketmq_producers_sup, []).

init([]) ->
%%  新建主题ets表
  ets:new(rocketmq_topic, [public, named_table]),
%%  进程规范
  SupFlags = #{strategy => one_for_one, intensity => 10, period => 5},
%%  启动时候没有子进程
  Children = [],
  {ok, {SupFlags, Children}}.

ensure_present(ClientId, ProducerGroup, Topic, ProducerOpts) ->
  ChildSpec = child_spec(ClientId, ProducerGroup, Topic, ProducerOpts),
  case supervisor:start_child(rocketmq_producers_sup, ChildSpec) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {ok, Pid};
    {error, {{already_started, Pid}, _}} -> {ok, Pid};
    {error, already_present} -> {error, not_running}
  end.

ensure_absence(ClientId, Name) ->
  Id = {ClientId, Name},
  case supervisor:terminate_child(rocketmq_producers_sup,
    Id)
  of
    ok ->
      ok = supervisor:delete_child(rocketmq_producers_sup,
        Id);
    {error, not_found} -> ok
  end.

child_spec(ClientId, ProducerGroup, Topic,
    ProducerOpts) ->
  #{id => {ClientId, get_name(ProducerOpts)},
    start =>
    {rocketmq_producers,
      start_link,
      [ClientId, ProducerGroup, Topic, ProducerOpts]},
    restart => transient, type => worker,
    modules => [rocketmq_producer]}.

get_name(ProducerOpts) ->
  maps:get(name, ProducerOpts, rocketmq_producers).


