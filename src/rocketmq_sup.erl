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

-vsn("4.2.1").

start_link() ->
  supervisor:start_link({local, rocketmq_sup}, rocketmq_sup, []).

init([]) ->
  SupFlags = #{strategy => one_for_all, intensity => 10,period => 5},
  Children = [client_sup(), producers_sup()],
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