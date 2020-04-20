%%%-------------------------------------------------------------------
%% @doc db top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(db_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    {ok, Pools} = application:get_env(db, pools),
    PoolSpecs = lists:map(fun({Name, PoolArgs, WorkerArgs}) ->
        poolboy:child_spec(Name, PoolArgs, WorkerArgs)
    end, Pools),
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    ChildSpecs = PoolSpecs,
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
