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
    variables_hash_bucket_size 128;


--- config
    location /count/ {
        content_by_lua_block {
            local counter = require "resty.counter"
            local c = counter.new("test", 2, 3, {{host="127.0.0.1"}})
            c:incr(ngx.var.uri)
            ngx.print(c:get(ngx.var.uri))
            ngx.exit(ngx.HTTP_OK)
        }
    }
    location /t {
        access_by_lua_block {
            ngx.location.capture("/count/hi?ttt=hello")
            ngx.location.capture("/count/hi?ttt=hello")
            ngx.location.capture("/count/hi?ttt=hello")

            ngx.location.capture("/count/foo/bar?ttt=hello")
            ngx.sleep(2)

            local res = ngx.location.capture("/count/hi?ttt=hello")
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



=== TEST 2: health check
--- http_config
    lua_package_path 'lib/?.lua;;';
    # lua_shared_dict test 10m;
    variables_hash_bucket_size 128;

--- config
    location /count/ {
        content_by_lua_block {
            local counter = require "resty.counter"
            local c = counter.new("test", 2, 3, {{host="127.0.0.1", port=6380, max_fails=3, fail_timeout=10},{host="127.0.0.1", port=6379, max_fails=3, fail_timeout=10}})
            c:incr(ngx.var.uri)
            ngx.print(c:get(ngx.var.uri))
            ngx.exit(ngx.HTTP_OK)
        }
    }
    location /t {
        access_by_lua_block {
            ngx.location.capture("/count/hello?ttt=hello")
            ngx.location.capture("/count/hello?ttt=hello")
            ngx.location.capture("/count/hello?ttt=hello")
            ngx.location.capture("/count/hello?ttt=hello")
            ngx.location.capture("/count/hello?ttt=hello")

            ngx.location.capture("/count/foo/bar/test?ttt=hello")
            ngx.location.capture("/count/foo/bar/test?ttt=hello")
            ngx.location.capture("/count/foo/bar/test?ttt=hello")
            ngx.sleep(2)

            local res = ngx.location.capture("/count/hello?ttt=hello")
            local res2 = ngx.location.capture("/count/foo/bar/test")
            ngx.print(res.body .. "_" .. res2.body)
            ngx.exit(200)
        }
    }

--- request
GET /t
--- response_body: 6_4failed health check
--- timeout: 20s
--- error_code: 200
--- error_log
Connection refused
