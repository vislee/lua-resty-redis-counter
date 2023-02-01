# lua-resty-redis-counter
sliding-window counter for ngx_lua based on redis.

Table of Contents
=================

* [Synopsis](#synopsis)
* [Methods](#methods)
    * [get_redis_conn](#get_redis_conn)
    * [new](#new)
    * [set_get_redis_conn_handler](#set_get_redis_conn_handler)
    * [incr](#incr)
    * [get](#get)
    * [close](#close)
* [Author](#author)
* [Copyright and License](#copyright-and-license)

Synopsis
========

```nginx
    location / {
        content_by_lua_block {
            local counter = require "resty.counter"
            local c = counter.new("test", 2, 5, {{host="127.0.0.1"}})
            c:incr(ngx.var.uri)
            ngx.print(c:get(ngx.var.uri))
            ngx.exit(ngx.HTTP_OK)
        }
    }
```


[Back to TOC](#table-of-contents)


Methods
=======


get_redis_conn
--------------
`syntax: redis, release = counter.get_redis_conn(opt)`

Get a connected redis object and `release` handler.
In case of failures, returns `nil` and a string describing the error.

The optional `opt` argument is a Lua table holding the following keys:

* `host`

	the redis host.

* `port`

	the redis port, defaults is 6379.

* `connect_timeout`

	the connect timeout(in ms), defaults is 1000.

* `send_timeout`

	the send timeout(in ms), defaults is 1000.

* `read_timeout`

	the read timeout(in ms), defaults is 1000.

* `passwd`

	the redis password.

* `db`

	the redis db.

* `max_fails`

	Passive health check, the number of unsuccessful.

* `fail_timeout`

	Passive health check, the duration of unsuccessful.


new
---
`syntax: ct = counter.new(name, wind, number, opts)`

Creates a counter object.
`name` is counter object names.
`wind` is sliding-window size. `number` is sliding-window number. So, the statistical duration is `wind` * `number`.
`opts` is array of redis cluster opt.


set_get_redis_conn_handler
--------------------------
`syntax: ct:set_get_redis_conn_handler(get_x_redis_conn)`


Sets the callback function to get the redis connection. defaults is `get_hash_redis_conn`.


incr
----
`syntax: ct:incr(key, value?)`

The counts incr in `key`.


get
---
`syntax: count, err = ct:get(key)`

Gets the count of `key`.


close
-----
`syntax: ct:close()`

Closes the counter object.


[Back to TOC](#table-of-contents)


Author
======

wenqiang li(vislee)

[Back to TOC](#table-of-contents)



Copyright and License
=====================

This module is licensed under the GPL license.

Copyright (C) 2022-, by vislee.

All rights reserved.

[Back to TOC](#table-of-contents)
