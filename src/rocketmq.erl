%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 13:17
%%%-------------------------------------------------------------------
-module(rocketmq).

-export([start/0]).

-export([ensure_supervised_client/2,
  ensure_supervised_client/3,
  stop_and_delete_supervised_client/1]).

-export([ensure_supervised_producers/4,
  stop_and_delete_supervised_producers/1]).

-export([send/2, send_sync/3]).

-vsn("4.2.1").

start() -> application:start(rocketmq).

ensure_supervised_client(ClientId, Opts) ->
  rocketmq_client_sup:ensure_present(ClientId,[{"127.0.0.1", 9876}], Opts).

ensure_supervised_client(ClientId, Hosts, Opts) ->
  rocketmq_client_sup:ensure_present(ClientId, Hosts, Opts).

stop_and_delete_supervised_client(ClientId) ->
  rocketmq_client_sup:ensure_absence(ClientId).

ensure_supervised_producers(ClientId, ProducerGroup, Topic, Opts) ->
  rocketmq_producers:start_supervised(ClientId, ProducerGroup, Topic, Opts).

stop_and_delete_supervised_producers(Producers) ->
  rocketmq_producers:stop_supervised(Producers).

send(Producers, Message) ->
  {_Partition, ProducerPid} = rocketmq_producers:pick_producer(Producers),
  rocketmq_producer:send(ProducerPid, Message).

send_sync(Producers, Message, Timeout) ->
  {_Partition, ProducerPid} = rocketmq_producers:pick_producer(Producers),
  rocketmq_producer:send_sync(ProducerPid, Message, Timeout).