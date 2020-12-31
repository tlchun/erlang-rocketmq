%%%-------------------------------------------------------------------
%%% @author Administrator
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 12月 2020 13:12
%%%-------------------------------------------------------------------
-module(rocketmq_protocol_frame).

-export([get_routeinfo_by_topic/2,
  send_message_v2/5,
  send_batch_message_v2/5,
  heart_beat/3]).

-export([parse/1]).

-vsn("4.2.1").
%% 获取路由信息通过主题
get_routeinfo_by_topic(Opaque, Topic) ->
  serialized(105, Opaque, [{<<"extFields">>, [{<<"topic">>, Topic}]}], <<"">>).

%% 发送消息
send_message_v2(Opaque, ProducerGroup, Topic, QueueId, {Payload, Properties}) ->
  Header = [{<<"a">>, ProducerGroup},
    {<<"b">>, Topic},
    {<<"e">>, integer_to_binary(QueueId)},
    {<<"i">>, Properties},
    {<<"g">>, integer_to_binary(erlang:system_time(millisecond))}],
  serialized(310, Opaque, [{<<"extFields">>, Header ++ message_base()}], Payload).

%% 批量发送
send_batch_message_v2(Opaque, ProducerGroup, Topic, QueueId, Payloads) ->
  send_message_v2(Opaque,ProducerGroup,Topic, QueueId, {batch_message(Payloads), <<>>}).

%% 心跳
heart_beat(Opaque, ClientID, GroupName) ->
  Payload = [{<<"clientID">>, ClientID}, {<<"consumerDataSet">>, []},
    {<<"producerDataSet">>, [[{<<"groupName">>, GroupName}], [{<<"groupName">>, <<"CLIENT_INNER_PRODUCER">>}]]}],
  serialized(34, Opaque, jsone:encode(Payload)).

%% 解析
parse(<<Len:32, HeaderLen:32,
  HeaderData:HeaderLen/binary, Bin/binary>>) ->
  case Bin == <<>> of
    true -> {jsone:decode(HeaderData), undefined, Bin};
    false ->
      case Len - 4 - HeaderLen of
        0 -> {jsone:decode(HeaderData), undefined, Bin};
        PayloadLen ->
          <<Payload:PayloadLen/binary, Bin1/binary>> = Bin,
          {jsone:decode(HeaderData), jsone:decode(Payload), Bin1}
      end
  end;
parse(Bin) -> {undefined, undefined, Bin}.

batch_message(Payloads) -> batch_message(Payloads, <<>>).

batch_message([], Acc) -> Acc;
batch_message([{Payload, Properties} | Payloads], Acc) ->
  MagicCode = 0,
  Crc = 0,
  PayloadLen = size(Payload),
  Properties = <<>>,
  PropertiesLen = size(Properties),
  Len = 10 + PayloadLen + size(Properties),
  NewAcc = <<Acc/binary, Len:32, MagicCode:32, Crc:32,
    PayloadLen:32, Payload/binary, PropertiesLen:16>>,
  batch_message(Payloads, NewAcc).

serialized(Code, Opaque, Payload) ->
  serialized(Code, Opaque, [], Payload).

serialized(Code, Opaque, Header0, Payload) ->
  Header = [{<<"code">>, Code}, {<<"opaque">>, Opaque}] ++ Header0 ++ header_base(),
  HeaderData = jsone:encode(Header),
  HeaderLen = size(HeaderData),
  Len = 4 + HeaderLen + size(Payload),
  <<Len:32, HeaderLen:32, HeaderData/binary, Payload/binary>>.

header_base() ->
  [{<<"flag">>, 0},
    {<<"language">>, <<"JAVA">>},
    {<<"serializeTypeCurrentRPC">>, <<"JSON">>},
    {<<"version">>, 315}].

message_base() ->
  [{<<"c">>, <<"TBW102">>},
    {<<"d">>, 8},
    {<<"f">>, 0},
    {<<"h">>, 0},
    {<<"j">>, 0},
    {<<"k">>, <<"false">>},
    {<<"m">>, <<"false">>}].
