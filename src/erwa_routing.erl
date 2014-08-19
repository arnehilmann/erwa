-module(erwa_routing).

-export([initialize/0]).
-export([start_realm/1]).
-export([stop_realm/1]).
-export([handle_wamp_message/1]).




-define(ROUTER_DETAILS,[
                        {agent,<<"Erwa-0.0.1">>},
                        {roles,[
                                {broker,[{features,[
                                                    {subscriber_blackwhite_listing,false},
                                                    {publisher_exclusion,true},
                                                    {publisher_identification,true},
                                                    {publication_trustlevels,false},
                                                    {pattern_based_subscription,false},
                                                    {partitioned_pubsub,false},
                                                    {subscriber_metaevents,false},
                                                    {subscriber_list,false},
                                                    {event_history,false}
                                                    ]} ]},
                                {dealer,[{features,[
                                                    {callee_blackwhite_listing,false},
                                                    {caller_exclusion,false},
                                                    {caller_identification,true},
                                                    {call_trustlevels,false},
                                                    {pattern_based_registration,false},
                                                    {partitioned_rpc,false},
                                                    {call_timeout,false},
                                                    {call_canceling,false},
                                                    {progressive_call_results,true}
                                                    ]}]}]}]).


-record(realm, {
  name = undefined,
  accept_new = true
}).

-record(session, {
  id = undefined,
  pid = undefined,
  realm = undefined,

  details = undefined,
  requestId = 1,
  goodbye_sent = false,
  subscriber_blackwhite_listing,
  subscriptions = [],
  registrations = []
}).

-record(publication ,{
  id = undefined,
  url = undefined,


}).

-record(topic, {
  id = undefined,
  url = undefined,

  publishId = 1,
  subscribers = [],
  options = undefined
}).


-record(procedure, {
  id = undefined,
  url = undefined,

  options = undefined,
  session_id = undefined
}).


-record(invocation, {
  id = undefined,
  callee_pid = undefined,
  request_id = undefined,
  caller_pid = undefined,
  progressive = false
}).

initialize() ->
  case is_fresh_startup() of
    true -> init_db();
    {exists, Tbls} ->
       ok = mnesia:wait_for_tables(Tbls, 1000000)
  end.

start_realm(Name) ->
  T = fun() ->
        ok = mnesia:write(#realm{name=Name})
      end,
  mnesia:transaction(T).

stop_realm(Name) ->
  T = fun() ->
        ok = mnesia:delete(realm,Name)
      end,
  mnesia:transaction(T).


-spec handle_wamp_message(Msg :: term()) -> ok.
handle_wamp_message({hello,Realm,Details}) ->
  {ok,SessionId} = create_session(self(),Details),
  send_message_to({welcome,SessionId,?ROUTER_DETAILS},self());
  %send_message_to({challenge,wampcra,[{challenge,JSON-Data}]},self());

handle_wamp_message({authenticate,_Signature,_Extra}) ->
  send_message_to({abort,[],not_authorized},self());

handle_wamp_message({goodbye,_Details,_Reason}) ->
  Session = get_session_from_pid(self(),State),
  SessionId = Session#session.id,
  case Session#session.goodbye_sent of
    true ->
      ok;
    _ ->
      T = fun() ->
            ok = mnesia:write(session,Session#session{goodbye_sent=true})
          end,
      mnesia:transaction(T),
      send_message_to({goodbye,[],goodbye_and_out},self())
  end,
  send_message_to(shutdown,self()),
  ok;

handle_wamp_message({publish,_RequestId,Options,Topic,Arguments,ArgumentsKw}) ->
  {ok,_PublicationId} = send_event_to_topic(Options,Topic,Arguments,ArgumentsKw,State),
  % TODO: send a reply if asked for ...
  ok;

handle_wamp_message({subscribe,RequestId,Options,Topic}) ->
  {ok,TopicId} = subscribe_to_topic(Pid,Options,Topic,State),
  send_message_to({subscribed,RequestId,TopicId},self());
  ok;

handle_wamp_message({unsubscribe,RequestId,SubscriptionId}) ->
  case unsubscribe_from_topic(Pid,SubscriptionId,State) of
    true ->
      send_message_to({unsubscribed,RequestId},self());
    false ->
      send_message_to({error,unsubscribe,RequestId,[],no_such_subscription},self())
  end;
  ok;

handle_wamp_message({call,RequestId,Options,Procedure,Arguments,ArgumentsKw}) ->
  %case enqueue_procedure_call( Pid, RequestId, Options,Procedure,Arguments,ArgumentsKw,State) of
%    true ->
%      ok;
%    false ->
%      send_message_to({error,call,RequestId,[],no_such_procedure,undefined,undefined},Pid)
%  end;
  ok;

handle_wamp_message({register,RequestId,Options,Procedure}) ->
  %case register_procedure(Pid,Options,Procedure,State) of
%    {ok,RegistrationId} ->
%      send_message_to({registered,RequestId,RegistrationId},Pid);
%    {error,procedure_already_exists} ->
%      send_message_to({register,error,RequestId,[],procedure_already_exists,undefined,undefined},Pid)
%  end;
  ok;

handle_wamp_message({unregister,RequestId,RegistrationId}) ->
  %case unregister_procedure(Pid,RegistrationId,State) of
%    true ->
%      send_message_to({unregistered,RequestId},Pid);
%    false ->
%      send_message_to({error,unregister,RequestId,[],no_such_registration,undefined,undefined},Pid)
%  end;
  ok;

handle_wamp_message({error,invocation,InvocationId,Details,Error,Arguments,ArgumentsKw}) ->
  %case dequeue_procedure_call(Pid,InvocationId,Details,Arguments,ArgumentsKw,Error,State) of
%    {ok} -> ok;
%    {error,not_found} -> ok;
%    {error,wrong_session} -> ok
%  end;
  ok;

handle_wamp_message({yield,InvocationId,Options,Arguments,ArgumentsKw}) ->
%  case dequeue_procedure_call(Pid,InvocationId,Options,Arguments,ArgumentsKw,undefined,State) of
%    {ok} -> ok;
%    {error,not_found} -> ok;
%    {error,wrong_session} -> ok
%  end;
  ok;

handle_wamp_message(Msg) ->
  io:format("unknown message ~p~n",[Msg]),
  ok.


-spec create_session(Pid :: pid(), Details :: list()) -> {ok,non_neg_integer()}.
create_session(Pid,Details) ->
  Id = gen_id(),
  T = fun() ->
                  ok = mnesia:write(#session{id=Id,pid=Pid,details=Details})
                  end,
  case mnesia:transaction(T) of
    {atomic,ok} -> {ok,Id};
    {aborted,_} -> create_session(Pid,Details)
  end.


-spec send_event_to_topic(Options :: list(), Url :: binary(), Arguments :: list()|undefined, ArgumentsKw :: list()|undefined) -> {ok,non_neg_integer()}.
send_event_to_topic(Options,Url,Arguments,ArgumentsKw) ->
  PublicationId =
    case ets:lookup(Ets,Url) of
      [] ->
        gen_id();
      [UrlTopic] ->
        TopicId = UrlTopic#url_topic.topic_id,
        [Topic] = ets:lookup(Ets,TopicId),
        IdToPid = fun(Id,Pids) -> [#session{pid=Pid}] = ets:lookup(Ets,Id), [Pid|Pids] end,
        Session = get_session_from_pid(FromPid,State),
        Peers =
          case lists:keyfind(exclude_me,1,Options) of
            {exclude_me,false} ->
                lists:foldl(IdToPid,[],Topic#topic.subscribers);
            _ -> lists:delete(FromPid,lists:foldl(IdToPid,[],Topic#topic.subscribers))
          end,
        SubscriptionId = Topic#topic.id,
        PublishId = gen_id(),
        Details1 =
          case lists:keyfind(disclose_me,1,Options) of
            {disclose_me,true} -> [{publisher,Session#session.id}];
            _ -> []
          end,
        Message = {event,SubscriptionId,PublishId,Details1,Arguments,ArgumentsKw},
        send_message_to(Message,Peers),
        PublishId
    end,
  {ok,PublicationId}.


-spec subscribe_to_topic(Options :: list(), Url :: binary()) -> {ok, non_neg_integer()}.
subscribe_to_topic(Options,Url) ->
  Session = get_session_from_pid(self()),
  SessionId = Session#session.id,
  Subs = Session#session.subscriptions,
  Topic =
    case ets:lookup(Ets,Url) of
      [] ->
        % create the topic ...
        {ok,T} = create_topic(Url,Options),
        T;
      [UrlTopic] ->
        Id = UrlTopic#url_topic.topic_id,
        [T] = ets:lookup(Ets,Id),
        T
    end,
  #topic{id=TopicId,subscribers=Subscribers} = Topic,
  ets:update_element(Ets,TopicId,{#topic.subscribers,[SessionId|lists:delete(SessionId,Subscribers)]}),
  ets:update_element(Ets,SessionId,{#session.subscriptions,[TopicId|lists:delete(TopicId,Subs)]}),
  {ok,TopicId}.


-spec create_topic(Url :: binary(), Options :: list) -> {ok,#topic{}}.
create_topic(Url,Options) ->
  Id = gen_id(),
  T = #topic{id=Id,url=Url,options=Options},
  Trans = fun() ->
            [] = mnesia:read(topic,Id),
            ok = mnesia:write(T),
          end,
  Topic =
    case mnesia:transaction(Trans) of
      true ->
        true = ets:insert_new(Ets,#url_topic{url=Url,topic_id=Id}),
        T;
      false -> create_topic(Url,Options,State)
    end,
  {ok,Topic}.


-spec unsubscribe_from_topic(Pid :: pid(), SubscriptionId :: non_neg_integer()) -> true | false.
unsubscribe_from_topic(Pid,SubscriptionId) ->
  Session = get_session_from_pid(Pid),
  case lists:member(SubscriptionId,Session#session.subscriptions) of
    false ->
      false;

    true ->
      ok = remove_session_from_topic(Session,SubscriptionId),
      true
  end.

-spec remove_session_from_topic(Session :: #session{}, TopicId :: non_neg_integer()) -> ok | not_found.
remove_session_from_topic(Session,TopicId) ->
  SessionId = Session#session.id,
  [Topic] = ets:lookup(Ets,TopicId),
  ets:update_element(Ets,TopicId,{#topic.subscribers,lists:delete(SessionId,Topic#topic.subscribers)}),
  ets:update_element(Ets,SessionId,{#session.subscriptions,lists:delete(TopicId,Session#session.subscriptions)}),
  ok.

-spec send_message_to(Msg :: term(), Peer :: list() |  pid()) -> ok.
send_message_to(Msg,Pid) when is_pid(Pid) ->
  send_message_to(Msg,[Pid]);
send_message_to(Msg,Peers) when is_list(Peers) ->
  Send = fun(Pid) ->
           Pid ! {erwa,Msg} end,
  lists:foreach(Send,Peers),
  ok.

-spec get_session_from_pid(Pid :: pid()) -> #session{}|undefined.
get_session_from_pid(Pid) ->
  Q = qlc:q([S || S <- mnesia:table(session), S#session.pid = Pid]),
  T = fun() ->
        qlc:e(Q)
      end,
  case mnesia:transaction(T) of
    [Session] -> Session;
    _ -> undefined
  end.

-spec gen_id() -> non_neg_integer().
gen_id() ->
  crypto:rand_uniform(0,9007199254740992).

is_fresh_startup() ->
  Node = node(),
  case mnesia:system_info(tables) of
    [schema] -> true;
    Tbls ->
      case mnesia:table_info(schema, cookie) of
        {_, Node}  -> {exists, Tbls};
        _            -> true
      end
  end.

init_db() ->
  mnesia:stop(),
  mnesia:create_schema([node()]),
  mnesia:start(),

  {atomic,ok} = mnesia:create_table(realm,[{attributes,record_info(fields,realm)},
                                             {type,set}]),
  {atomic,ok} = mnesia:create_table(session,[{attributes,record_info(fields,session)},
                                             {index,[pid,realm]},
                                             {type,set}]),
  {atomic,ok} = mnesia:create_table(topic,[{attributes,record_info(fields,topic)},
                                           {index,[url]},
                                           {type,set}]),
  {atomic,ok} = mnesia:create_table(procedure,[{attributes,record_info(fields,procedure)},
                                               {index,[url]},
                                               {type,set}]),
  {atomic,ok} = mnesia:create_table(invocation,[{attributes,record_info(fields,invocation)},
                                                {type,set}]),
  ok.
