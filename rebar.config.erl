-module('rebar.config').

-export([do/2]).

do(Dir, Config) ->
    Config ++ coveralls().

coveralls() ->
    case {os:getenv("GITHUB_ACTIONS"), os:getenv("GITHUB_TOKEN")} of
        {"true", Token} when is_list(Token) ->
            Cfgs = [
                {coveralls_repo_token, Token},
                {coveralls_service_job_id, os:getenv("GITHUB_RUN_ID")},
                {coveralls_commit_sha, os:getenv("GITHUB_SHA")},
                {coveralls_service_number, os:getenv("GITHUB_RUN_NUMBER")}
            ],
            case
                os:getenv("GITHUB_EVENT_NAME") =:= "pull_request" andalso
                    string:tokens(os:getenv("GITHUB_REF"), "/")
            of
                [_, "pull", PRNO, _] ->
                    [{coveralls_service_pull_request, PRNO} | Cfgs];
                _ ->
                    Cfgs
            end;
        _ ->
            []
    end.
