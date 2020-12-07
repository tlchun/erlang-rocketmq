%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:52
%%%-------------------------------------------------------------------

-file("rocketmq_app.erl", 1).

-module(rocketmq_app).

-behaviour(application).

-export([start/2, stop/1]).

-vsn("4.2.1").

start(_, _) -> rocketmq_sup:start_link().

stop(_) -> ok.



%% {ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(Beam,[abstract_code]).
%% io:fwrite("~s~n", [erl_prettypr:format(erl_syntax:form_list(AC))]).

{ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(emqx_bridge_rocket,[abstract_code]).

{ok,{_,[{abstract_code,{_,AC}}]}} = beam_lib:chunks(rocketmq_producer,[abstract_code]).
