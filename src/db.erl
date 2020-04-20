%%%-------------------------------------------------------------------
%%% @author ysx
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Oct 2019 15:24
%%%-------------------------------------------------------------------
-module(db).
-author("ysx").

%% ==================================================
%% API
%% ==================================================
-export([query/1, query/2, query_return_list/1]).
-export([query_data/1, query_data/2]).
-export([select/1, select/2, select_one/3, select_one/4]).
-export([where/1, where_or/1, where_in/1, where_in_by_query/2]).
-export([insert/2, update/2, replace/2, delete/1]).
-export([order_by/1, order_by_desc/1]).
-export([limit/2]).
-export([count/1]).
-export([truncate/1]).

query(Query) ->
    case mysql_poolboy:query(mysql, Query) of
        {ok, FieldList, UserList} ->
            phrase_data(FieldList, UserList);
        ok ->
            ok;
        Error ->
            Error
    end.

query_return_list(Query) ->
    case query(Query) of
        {ok, Maps} ->
            Maps;
        _ ->
            []
    end.

query(Query, Params) ->
    case mysql_poolboy:query(mysql, Query, Params) of
        {ok, FieldList, UserList} ->
            phrase_data(FieldList, UserList);
        Error ->
            Error
    end.

query_data(Query) ->
    case mysql_poolboy:query(mysql, Query) of
        {ok, _FieldList, UserList} ->
            {ok, UserList};
        Error ->
            Error
    end.

query_data(Query, Params) ->
    case mysql_poolboy:query(mysql, Query, Params) of
        {ok, _FieldList, UserList} ->
            {ok, UserList};
        Error ->
            Error
    end.


%% ==========================================================
%% return Query String.
%% ==========================================================
select(Table) ->
    "SELECT * " ++ " FROM `" ++ atom_to_list(Table) ++ "`".

select(Column, Table) ->
    ClmStr = string:join([to_string(Key) || Key <- Column], ","),
    "SELECT "  ++ ClmStr ++ " FROM `" ++ atom_to_list(Table) ++ "`".

select_one(Key, Table, Where) ->
    ClmStr = to_string(Key),
    "(SELECT "  ++ ClmStr ++ " FROM `" ++ atom_to_list(Table) ++ "`" ++ Where ++ ") AS " ++ ClmStr.
select_one(Key, Table, Where, As) ->
    ClmStr = to_string(Key),
    AsStr = to_string(As),
    "(SELECT "  ++ ClmStr ++ " FROM `" ++ atom_to_list(Table) ++ "`" ++ Where ++ ") AS " ++ AsStr.


where(List) ->
    {Keys, Ops, Args} = lists:unzip3(List),
    WhereStr = " WHERE " ++ string:join([to_string(Key) ++ Op ++ "~s" ||
        {Key, Op} <- lists:zip(Keys, Ops) ], " AND "),
    format_sql(WhereStr, Args).

where_or(List) ->
    {Keys, Ops, Args} = lists:unzip3(List),
    WhereStr = " WHERE " ++ string:join([to_string(Key) ++ Op ++ "~s" ||
        {Key, Op} <- lists:zip(Keys, Ops) ], " OR "),
    format_sql(WhereStr, Args).

where_in({Key, Args}) ->
    WhereStr = " WHERE " ++ to_string(Key) ++ " IN ("  ++ string:join(["~s" ||
        _ <- Args ], ",") ++ ") ",
    format_sql(WhereStr, Args).

where_in_by_query(Key, Query) ->
    " WHERE " ++ to_string(Key) ++ " IN ("  ++ Query ++ ") ".

insert(Table, PropList) ->
    {Keys, Values} = format_data(PropList),
    Holds = string:join(lists:duplicate(length(Keys), "~s"), ", "),
    KeysStr = string:join([to_string(Key) || Key <- Keys], ","),
    InsertStr = "INSERT INTO `" ++ atom_to_list(Table) ++ "` (" ++ KeysStr ++ ") VALUES (" ++ Holds ++ ")",
    format_sql(InsertStr, Values).

update(Table, PropList) ->
    {Keys, Values} = format_data(PropList),
    SetStr = string:join([to_string(Key) ++ "=~s" || Key <- Keys], ","),
    InsertStr = "UPDATE `" ++ atom_to_list(Table) ++ "` SET " ++ SetStr,
    format_sql(InsertStr, Values).

replace(Table, PropList) ->
    {Keys, Values} = format_data(PropList),
    Holds = string:join(lists:duplicate(length(Keys), "~s"), ", "),
    KeysStr = string:join([atom_to_list(Key) || Key <- Keys], "`,`"),
    InsertStr = "REPLACE INTO `" ++ atom_to_list(Table) ++ "` (`" ++ KeysStr ++ "`) VALUES (" ++ Holds ++ ")",
    format_sql(InsertStr, Values).

delete(Table) ->
    "DELETE FROM `" ++ atom_to_list(Table) ++ "`".

order_by(Key) ->
    " ORDER BY " ++ to_string(Key).

order_by_desc(Key) ->
    " ORDER BY " ++ to_string(Key) ++ " DESC".


limit(Page, Column) ->
    io_lib:format(" LIMIT ~w, ~w ", [(Page - 1) * Column, Column]).

count(Table) ->
    "SELECT COUNT(*) " ++ " FROM " ++ to_string(Table).

truncate(Table) when  is_atom(Table) ->
    "TRUNCATE TABLE "++ to_string(Table);
truncate(Tables) ->
    lists:flatten([io_lib:format("TRUNCATE TABLE `~w`;", [Table]) || Table <- Tables]).

%% ==================================================
%% Internal
%% ==================================================

phrase_data(FieldList, UserList) ->
%%    io:format("FieldList:~p~n  UserList:~p~n",  [FieldList,  UserList]),
    DataList = [lists:zip([binary_to_atom(Key, utf8) || Key <- FieldList], User) || User <- UserList],
    Maps = lists:map(fun(X) ->
        X1 = null_to_undefined(X),
        maps:from_list(X1)
    end, DataList),
    {ok, Maps}.

null_to_undefined(List) ->
    lists:map(fun({K,V}) ->
        case V of
            null ->
                {K, undefined};
            _ ->
                {K,V}
        end
    end, List).

%% @spec format_sql(sql(), arguments()) -> binary()
%% @doc <pre>
%% 格式化sql语句
%% 变量用~s表示

format_sql(Sql, Args) when is_list(Sql) ->
    L = [encode(A) || A <- Args],
    format(Sql, L);

%% 	unicode:characters_to_binary(format(Sql, L), utf8);
format_sql(Sql, Args) when is_binary(Sql) ->
    format_sql(binary_to_list(Sql), Args).

%% @doc Encode a value so that it can be included safely in a MySQL query.
%%
%% @spec encode(Val::term(), AsBinary::bool()) ->
%%   string() | binary() | {error, Error}
encode(Val) ->
    encode(Val, false).
encode(Val, false) when Val == undefined; Val == null ->
    "null";
encode(Val, true) when Val == undefined; Val == null ->
    <<"null">>;
encode(Val, false) when is_binary(Val) ->
    binary_to_list(quote(Val));
encode(Val, true) when is_binary(Val) ->
    quote(Val);
encode(Val, true) ->
    list_to_binary(encode(Val,false));
encode(Val, false) when is_atom(Val) ->
    quote(atom_to_list(Val));
encode(Val, false) when is_list(Val) ->
    quote(Val);
encode(Val, false) when is_integer(Val) ->
    integer_to_list(Val);
encode(Val, false) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
encode({datetime, Val}, AsBinary) ->
    encode(Val, AsBinary);
encode({{Year, Month, Day}, {Hour, Minute, Second}}, false) ->
    Res = two_digits([Year, Month, Day, Hour, Minute, Second]),
    lists:flatten(Res);
encode({TimeType, Val}, AsBinary)
    when TimeType == 'date';
    TimeType == 'time' ->
    encode(Val, AsBinary);
encode({Time1, Time2, Time3}, false) ->
    Res = two_digits([Time1, Time2, Time3]),
    lists:flatten(Res);
encode(Val, _AsBinary) ->
    {error, {unrecognized_value, Val}}.

two_digits(Nums) when is_list(Nums) ->
    [two_digits(Num) || Num <- Nums];
two_digits(Num) ->
    Str = io_lib:format("~b", [Num]),
    case length(Str) of
        1 -> [$0 | Str];
        _ -> Str
    end.


%%  Quote a string or binary value so that it can be included safely in a
%%  MySQL query.
quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote(String, [])])];	%% 39 is $'
quote(Bin) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
    Acc;
quote([0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
    quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
    quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).


%% 格式化字符串
format(Format, []) ->
    Format;
format(Format, Args) ->
    format(Format, Args, []).

format([$~, $s|T], [Arg|Args], Ret) when is_list(Arg) ->
    format(T, Args, [Arg|Ret]);
format([$~, $s|T], [Arg|Args], Ret) when is_integer(Arg) ->
    format(T, Args, [integer_to_list(Arg)|Ret]);
format([$~, $s|T], [Arg|Args], Ret) when is_float(Arg) ->
    format(T, Args, [float_to_list(Arg)|Ret]);
format([$~, $s|T], [Arg|Args], Ret) when is_atom(Arg) ->
    format(T, Args, [atom_to_list(Arg)|Ret]);
format([$~, $s|T], [_Arg|Args], Ret) ->
    format(T, Args, Ret);
format(_L=[$~, $s|_], [], _Ret) ->
    %lists:flatten(lists:reverse([L|Ret]));
    throw({error, bad_arity});
format([H|T], Args, Ret) ->
    format(T, Args, [H|Ret]);
format([], [_|_], _Ret) ->
    throw({error, bad_arity});
format([], _Args, Ret) ->
    lists:flatten(lists:reverse(Ret)).



%% 处理 #{} 和 [{key, val} | _]
format_data(Maps) when is_map(Maps) ->
    {maps:keys(Maps), maps:values(Maps)};
format_data(PropList) ->
    lists:unzip(PropList).


to_string(Key) when is_atom(Key) ->
    "`" ++ atom_to_list(Key) ++ "`";
to_string(Key) when is_binary(Key) ->
    binary_to_list(Key);
to_string(Key) ->
    Key.

