%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 12:59
%%%-------------------------------------------------------------------
-module(rocketmq_client).

-behaviour(gen_server).
-export([start_link/3]).
-export([get_routeinfo_by_topic/2]).
-export([get_status/1]).

-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

%% 进程状态记录
-record(state,
{
  requests, %% 请求
  opaque_id,%% 请求ID
  sock,     %% 客户端 socket
  servers,  %% 服务器配置
  opts,     %% 配置选项
  last_bin = <<>>
}).

%% 启动 rocketmq_client 客户端 工作进程
start_link(ClientId, Servers, Opts) ->
  gen_server:start_link({local, ClientId}, rocketmq_client, [Servers, Opts], []).

get_routeinfo_by_topic(Pid, Topic) ->
  gen_server:call(Pid, {get_routeinfo_by_topic, Topic}).

get_status(Pid) ->
  gen_server:call(Pid, get_status, 5000).

%% 启动服务进程回调
init([Servers, Opts]) ->
  %% 设置服务器参数到进程状态
  State = #state{servers = Servers, opts = Opts},
  %% 连接服务器
  case get_sock(Servers, undefined) of
    %% 连接失败
    error -> {error, fail_to_connect_rocketmq_server};
    %% 连接成功返回 socket，保存变量到进程状态记录中
    Sock -> {ok, State#state{sock = Sock, opaque_id = 1, requests = #{}}}
  end.

handle_call({get_routeinfo_by_topic, Topic}, From, State = #state{opaque_id = OpaqueId, sock = Sock, requests = Reqs, servers = Servers}) ->
  case get_sock(Servers, Sock) of
    error ->
      log_error("Servers: ~p down", [Servers]),
      {noreply, State};
    Sock1 ->
      Package = rocketmq_protocol_frame:get_routeinfo_by_topic(OpaqueId, Topic),
      gen_tcp:send(Sock1, Package),
      {noreply, next_opaque_id(State#state{requests = maps:put(OpaqueId, From, Reqs), sock = Sock1})}
  end;
handle_call(get_status, _From, State = #state{sock = undefined, servers = Servers}) ->
  case get_sock(Servers, undefined) of
    error -> {reply, false, State};
    Sock -> {reply, true, State#state{sock = Sock}}
  end;
handle_call(get_status, _From, State) ->
  {reply, true, State};
handle_call(_Req, _From, State) ->
  {reply, ok, State, hibernate}.

handle_cast(_Req, State) -> {noreply, State, hibernate}.

%% 处理socket 过来的数据
handle_info({tcp, _, Bin}, State) ->
  handle_response(Bin, State);

%% socket 关闭事件处理
handle_info({tcp_closed, Sock}, State = #state{sock = Sock}) ->
  {noreply, State#state{sock = undefined}, hibernate};

handle_info(_Info, State) ->
  log_error("RocketMQ client Receive unknown message:~p~n", [_Info]),
  {noreply, State, hibernate}.

terminate(_Reason, #state{}) -> ok.

code_change(_, State, _) -> {ok, State}.

%% 空数据处理
handle_response(<<>>, State) -> {noreply, State, hibernate};
%% 非空数据处理
handle_response(Bin, State = #state{requests = Reqs, last_bin = LastBin}) ->
  %% 解析协议
  case rocketmq_protocol_frame:parse(<<LastBin/binary, Bin/binary>>) of
    {undefined, undefined, Bin1} ->
      {nodelay, State#state{last_bin = Bin1}, hibernate};
    {Header, Payload, Bin1} ->
      NewReqs = do_response(Header, Payload, Reqs),
      handle_response(Bin1, State#state{requests = NewReqs, last_bin = <<>>})
  end.

do_response(Header, Payload, Reqs) ->
  %% 从 Header 获取 key 为 <<"opaque">> 的数据，如果为空，默认为 1
  OpaqueId = maps:get(<<"opaque">>, Header, 1),
  %% 获取 Reqs 里面 key 为 OpaqueId 的数据，如果为空，默认为 1 undefined
  case maps:get(OpaqueId, Reqs, undefined) of
    undefined -> Reqs;
    From ->
      %% 向 From 进程 回复消息
      gen_server:reply(From, {Header, Payload}),
      %% 从Reqs删除 OpaqueId的相关数据
      maps:remove(OpaqueId, Reqs)
  end.

tune_buffer(Sock) ->
  %% 接收buffer 和 发送 buffer 调整
  {ok, [{recbuf, RecBuf}, {sndbuf, SndBuf}]} = inet:getopts(Sock, [recbuf, sndbuf]),
  %% 设置socket 的 buffer ，取 接收和发送中最大的
  inet:setopts(Sock, [{buffer, max(RecBuf, SndBuf)}]).

get_sock(Servers, undefined) -> try_connect(Servers);
get_sock(_Servers, Sock) -> Sock.

%% 空参数匹配，返回错误
try_connect([]) -> error;
%% 匹配出服务的ip和端口
try_connect([{Host, Port} | Servers]) ->
  case gen_tcp:connect(Host, Port, [binary, {packet, raw}, {nodelay, true}, {active, true}, {reuseaddr, true}, {send_timeout, 60000}], 60000) of
    {ok, Sock} ->
      tune_buffer(Sock),
      %% 移交 Sock 到 本进程 发送给这个Socket的信息就相当于发送到绑定的进程Pid
      gen_tcp:controlling_process(Sock, self()),
      Sock;
    _Error -> try_connect(Servers)
  end.

log_error(Fmt, Args) ->
  error_logger:error_msg(Fmt, Args).

next_opaque_id(State = #state{opaque_id = 65535}) ->
  State#state{opaque_id = 1};
next_opaque_id(State = #state{opaque_id = OpaqueId}) ->
  State#state{opaque_id = OpaqueId + 1}.




