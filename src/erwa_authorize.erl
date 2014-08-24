-module(erwa_authorize).
-behaviour(gen_server).

-export([append/3]).
-export([prepend/3]).
-export([remove/1]).

-export([get_list/0]).



%% gen_server.
-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).
