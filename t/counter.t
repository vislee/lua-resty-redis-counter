use Test::Nginx::Socket::Lua;
use Cwd qw(cwd);

log_level('debug');

repeat_each(1);
plan tests => repeat_each() * (3 * blocks());

no_long_string();

run_tests();

__DATA__

=== TEST 1: count
--- http_config
    lua_package_path 'lib/?.lua;;';
    # lua_shared_dict test 10m;

--- config
    location /count/ {
        content_by_lua_block {
            local counter = require "resty.counter".new("test", 2, 2, {host="127.0.0.1"})
            counter:incr(ngx.var.uri)
            ngx.print(counter:get(ngx.var.uri))
            ngx.exit(ngx.HTTP_OK)
        }
    }
    location /t {
        access_by_lua_block {
            ngx.location.capture("/count/hello?ttt=hello")
            ngx.location.capture("/count/hello?ttt=hello")
            ngx.location.capture("/count/hello?ttt=hello")

            ngx.location.capture("/count/foo/bar?ttt=hello")
            ngx.sleep(2)

            local res = ngx.location.capture("/count/hello?ttt=hello")
            local res2 = ngx.location.capture("/count/foo/bar")
            ngx.print(res.body .. "_" .. res2.body)
            ngx.exit(200)
        }
    }

--- request
GET /t
--- response_body: 4_2
--- timeout: 20s
--- error_code: 200
--- no_error_log
[error]
