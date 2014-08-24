-module(erwa_routing).

-export([initialize/0]).
-export([start_realm/1]).
-export([stop_realm/1]).


-export([create_state/0]).
-export([handle_incomming_message/2]).
-export([handle_outgoing_message/2]).


-define(ROUTER_DETAILS,[
                        {agent,<<"Erwa-0.0.1">>},
                        {roles,[
                                {broker,[{features,[
                                                    {subscriber_blackwhite_listing,false},
                                                    {publisher_exclusion,false},
                                                    {publisher_identification,false},
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
                                                    {caller_identification,false},
                                                    {call_trustlevels,false},
                                                    {pattern_based_registration,false},
                                                    {partitioned_rpc,false},
                                                    {call_timeout,false},
                                                    {call_canceling,false},
                                                    {progressive_call_results,false}
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
  subscriber_blackwhite_listing,
  subscriptions = [],
  registrations = []
}).

-record(publication ,{
  id = undefined,
  url = undefined,


}).

-record(topic, {
  url = undefined,
  id = undefined,

  publishId = 1,
  subscribers = [],
  options = undefined
}).

-record(pattern_subscription, {
  id = undefined,
  url = undefined,
  pattern = undefined,

  topics = []
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

-record(state,{
  sess_id = undefined,
  goodbye_sent = false,
  publisher = undefined,
  subscriber = undefined,
  caller = undefined,
  callee = undefined,
  subscriptions = []
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
  %% @todo Need to add a timer to forcefully shut down all existing connections
  %% @todo update state of sessions that goodbye is sent
  T = fun() ->
        case mnesia:read(realm,Name) of
          [Realm] ->
            NewRealm = Realm#realm{accept_new = false},
            SessionPIDs = mnesia:select(session,[{#session{pid='$1', realm='$2', _='_'},[{'=','$2',Name}],['$1']}]),
            ok = mnesia:write(NewRealm),
            SessionPIDs
          _ ->
            []
        end
      end,
  Pids = mnesia:transaction(T),
  send_message_to({goodbye,[],close_realm},Pids).



-spec create_state() -> #state{}.
create_state() ->
  #state{}.


handle_incomming_message(Message,State) ->
  handle_wamp_message(Message,State).



handle_outgoing_message({goodbye,_Details,_Reason},State) ->
  State#state{goodbye_sent=true};
handle_outgoing_message(Message,State) ->
  State.


-spec handle_wamp_message(Msg :: term(), #state{}) -> {term() | noreply | [any()], #state{}}.
handle_wamp_message({hello,RealmName,Details},#state{sess_id=undefined}) ->

  %% @todo implement a way to chek if authentication is needed
  %send_message_to({challenge,wampcra,[{challenge,JSON-Data}]},self());
  T_Realm = fun() ->
              case mnesia:read(realm,RealmName) of
                [RealmData] -> RealmData;
                [] -> undefined;
                _ -> error
              end
            end,
  Realm = mnesia:transaction(T_Realm),
  RealmAccepting = Realm#realm.accept_new,

  %% @todo validate the peer details ... someday
  %NewState = validate_peer_details(Details);

  case RealmAccepting of
    true ->
      {ok,SessionId} = create_session(Realm,Details),
      NewState = State#state{sess_id=SessionId},
      {{welcome,SessionId,?ROUTER_DETAILS},NewState};
    _ ->
      {[{abort,[],no_such_realm},shutdown],State}
  end;
handle_wamp_message({hello,RealmName,Details},State) ->
  % if the hello message is sent twice close the connection
  {shutdown,State};

%handle_wamp_message({authenticate,_Signature,_Extra},_State) ->
%  send_message_to({abort,[],not_authorized},self());

handle_wamp_message({goodbye,_Details,_Reason},#state{goodbye_sent=GB_Sent}=State) ->
  case GB_Sent of
    true ->
      {shutdown,State};
      _ ->
        {[{goodbye,[],goodbye_and_out},shutdown],State#state{goodbye_sent=true}}
  end;

handle_wamp_message({subscribe,RequestId,Options,TopicUrl},#state{sess_id=SessionId,subscriptions=Subs}=State) ->
  % there are three different kinds of subscription
  % - basic subscription eg com.example.url
  % - pattern_based_subscription, using the details match "prefix" and "wildcard"
  % - partitioned ones using nkey and rkey ... not yet understood


  case proplist:get_value(match,Options,exact) of
    exact ->
      % a basic subscription
      {ok,TopicId,NewState} = subscribe_to_topic(TopicUrl,State),
      {{subscribed,RequestId,TopicId},NewState};

    %prefix ->
      % a prefix subscription


    %wildcard ->
      % a wildcard subscription

    _ ->
      % unsupported match
      {{error,subscribe,RequestId,[],invalid_argument},State}S
  end;


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



handle_wamp_message({unsubscribe,RequestId,SubscriptionId},#state{subscriptions=Subs} = State) ->
  case lists:member(SubscriptionId,Subs) of
    true ->
      %% @todo remove subscription from database
      {{unsubscribed,RequestId},State#state{subscriptions=lists:delete(SubscriptionId,Subs)}};
    false ->
      {{error,unsubscribe,RequestId,[],no_such_subscription},State)
  end;

%handle_wamp_message({publish,_RequestId,Options,Topic,Arguments,ArgumentsKw}) ->
%  {ok,_PublicationId} = send_event_to_topic(Options,Topic,Arguments,ArgumentsKw,State),
  % TODO: send a reply if asked for ...
%  ok;

%handle_wamp_message({call,RequestId,Options,Procedure,Arguments,ArgumentsKw}) ->
  %case enqueue_procedure_call( Pid, RequestId, Options,Procedure,Arguments,ArgumentsKw,State) of
%    true ->
%      ok;
%    false ->
%      send_message_to({error,call,RequestId,[],no_such_procedure,undefined,undefined},Pid)
%  end;

%handle_wamp_message({register,RequestId,Options,Procedure}) ->
  %case register_procedure(Pid,Options,Procedure,State) of
%    {ok,RegistrationId} ->
%      send_message_to({registered,RequestId,RegistrationId},Pid);
%    {error,procedure_already_exists} ->
%      send_message_to({register,error,RequestId,[],procedure_already_exists,undefined,undefined},Pid)
%  end;

%handle_wamp_message({unregister,RequestId,RegistrationId}) ->
  %case unregister_procedure(Pid,RegistrationId,State) of
%    true ->
%      send_message_to({unregistered,RequestId},Pid);
%    false ->
%      send_message_to({error,unregister,RequestId,[],no_such_registration,undefined,undefined},Pid)
%  end;


%handle_wamp_message({error,invocation,InvocationId,Details,Error,Arguments,ArgumentsKw}) ->
  %case dequeue_procedure_call(Pid,InvocationId,Details,Arguments,ArgumentsKw,Error,State) of
%    {ok} -> ok;
%    {error,not_found} -> ok;
%    {error,wrong_session} -> ok
%  end;


% handle_wamp_message({yield,InvocationId,Options,Arguments,ArgumentsKw}) ->
%  case dequeue_procedure_call(Pid,InvocationId,Options,Arguments,ArgumentsKw,undefined,State) of
%    {ok} -> ok;
%    {error,not_found} -> ok;
%    {error,wrong_session} -> ok
%  end;


handle_wamp_message(Msg,State) ->
  io:format("unknown message ~p~n",[Msg]),
  {shutdown,State}.



-spec create_session(Details :: list()) -> {ok,non_neg_integer()}.
create_session(Details) ->
  Id = gen_id(),
  T = fun() ->
        ok = mnesia:write(#session{id=Id,pid=self(),details=Details})
      end,
  case mnesia:transaction(T) of
    {atomic,ok} -> {ok,Id};
    {aborted,_} -> create_session(Pid,Details)
  end.

-spec subscribe_to_topic(TopicUrl :: binary() ,State :: #state{}) -> {ok,non_neg_integer(),#state{}}.
subscribe_to_topic(TopicUrl,State) ->
  T = fun() ->
        case mnesia:read(topic,TopicUrl,write) of
          [] ->
            mnesia:
          [Topic] ->

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
