%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:52
%%%-------------------------------------------------------------------

-module(rocketmq_app).

-behaviour(application).

-export([start/2, stop/1]).

-vsn("4.2.1").

start(_, _) -> rocketmq_sup:start_link().

stop(_) -> ok.

