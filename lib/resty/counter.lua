-- Copyright (C) vislee

local lrucache = require "resty.lrucache"
local redis = require "resty.redis"

local ceil = math.ceil
local floor = math.floor
local str_fmt = string.format
local tab_concat = table.concat
local ngx_now = ngx.now
local ngx_crc32 = ngx.crc32_long

local _M = {}
local mt = { __index = _M }

local global = {}
local anchor_ts = 1530460800

local redis_counter_incrby_script = [==[
local key = KEYS[1]
local val, ttl = tonumber(ARGV[1]), tonumber(ARGV[2])

local res = redis.pcall('INCRBY', key, val)
if type(res) == "table" and res.err then
    return {err=res.err}
end
if res == val then
    local res = redis.pcall('EXPIRE', key, ttl)
    if type(res) == "table" and res.err then
        return {err=res.err}
    end
end
return res
]==]


local redis_counter_incrby = function(red, sha, key, val, ttl)
    if not sha then
        local res, err = red:script("LOAD", redis_counter_incrby_script)
        if not res then
            return nil, err
        end
        sha = res
    end

    local res, err = red:evalsha(sha, 1, key, val, ttl)
    if not res then
        return nil, err
    end

    return sha, res
end


local _get_redis_conn = function(opt)
    if opt == nil or type(opt) ~= "table" then
        return nil, "wrong opt"
    end

    if opt.host == nil then
        return nil, "opt.host is nil"
    end

    if opt.max_fails and opt.fail_timeout and not (opt.accessed and opt.checked and opt.fails) then
        opt.accessed = 0
        opt.checked = 0
        opt.fails = 0
    end

    local now = ngx_now()
    if opt.max_fails and opt.fail_timeout and opt.fails >= opt.max_fails and now - opt.checked <= opt.fail_timeout then
        return nil, "failed health check"
    end

    local red = redis:new()
    red:set_timeouts(opt.connect_timeout or 1000, opt.send_timeout or 1000, opt.read_timeout or 1000)
    local ok, err = red:connect(opt.host, (opt.port or 6379))
    if not ok then
        ngx.log(ngx.WARN, "failed to connect ", opt.host, ":", (opt.port or 6379), ". ", err)

        if opt.max_fails and opt.fail_timeout then
            opt.fails = opt.fails + 1
            opt.accessed = now
            opt.checked = now
        end

        return nil, err
    end

    if opt.max_fails and opt.fail_timeout and now - opt.checked > opt.fail_timeout then
        opt.checked = now
    end

    if opt.passwd and #opt.passwd > 0 then
        local res, err = red:auth(opt.passwd)
        if not res then
            return nil, err
        end
    end

    if opt.db and opt.db > 0 then
        red:select(opt.db)
    end

    if opt.max_fails and opt.fail_timeout and opt.accessed < opt.checked then
        opt.fails = 0
    end

    return red,
    function(failed)
        if failed then
            if opt.max_fails and opt.fail_timeout then
                local now = ngx_now()
                opt.fails = opt.fails + 1
                opt.accessed = now
                opt.checked = now
            end
            red:close()
            return
        end

        if opt.max_fails and opt.fail_timeout and opt.accessed < opt.checked then
            opt.fails = 0
        end

        local ok, err = red:set_keepalive(10000, 32)
        if not ok then
            ngx.log(ngx.WARN, "failed to set keepalive: ", err)
            red:close()
        end
    end,
    opt
end
_M.get_redis_conn = _get_redis_conn


local _get_hash_redis_conn = function(opts)
    return function(key)
        local idx = ngx_crc32(key) % #opts + 1
        return _get_redis_conn(opts[idx])
    end
end


function _M.new(name, wind, number, opts)
    local obj = global[name]
    if obj and obj.wind == wind and obj.wnum == number then
        return obj
    elseif obj then
        return nil, "wrong wind or number"
    end

    local cache
    if name and ngx.shared[name] then
        cache = ngx.shared[name]
    else
        cache = lrucache.new(1024)
    end

    obj = setmetatable({
        name = name,
        wcount = 0,
        cache = cache,
        wind = wind,
        midx = floor(86400/wind),
        wnum = number,
        redis_opts = opts,
        get_redis_conn_handler = _get_hash_redis_conn(opts),
    }, mt)
    global[name] = obj

    return obj
end


function _M.set_get_redis_conn_handler(self, get_x_redis_conn)
    self.get_redis_conn_handler = get_x_redis_conn(self.opts)
end


function _M.close(self)
    self.wcount = 0
    self.cache:flush_all()
    global[self.name] = nil
end


function _M.incr(self, key, value)
    local day_sec = ceil((ngx.now() - anchor_ts) % 86400)
    local day_sec_index = floor(day_sec/self.wind)

    local incr_key = tab_concat({self.name, key, str_fmt("%05d", day_sec_index)}, '_')
    local incr_val = self.cache:get(incr_key) or 0
    ngx.log(ngx.DEBUG, "cache:get incr_key:", incr_key, " incr_val:", incr_val)

    local incr_pre_key
    local incr_pre_val = 0
    -- change the count window
    if incr_val == 0 then
        local day_sec_pre_index = day_sec_index - 1
        if day_sec_pre_index < 0 then
            day_sec_pre_index = self.midx
        end
        incr_pre_key = tab_concat({self.name, key, str_fmt("%05d", day_sec_pre_index)}, '_')
        incr_pre_val = self.cache:get(incr_pre_key) or 0
        ngx.log(ngx.DEBUG, "cache:get incr_pre_key:", incr_pre_key, " incr_pre_val:", incr_pre_val)
    end

    -- incrby into redis
    if incr_pre_val > 0 then
        local redis, release, opt = self.get_redis_conn_handler(tab_concat({self.name, key}, '_'))
        if redis then
            local ttl = self.wind * self.wnum + 3
            local res, err = redis_counter_incrby(redis, opt.incrby_script_sha, incr_pre_key, incr_pre_val, ttl)
            if not res then
                opt.incrby_script_sha = nil
                release(true)
                ngx.log(ngx.WARN, "redis:script:incrby key:", incr_pre_key, " val:", incr_pre_val, " ttl:", ttl, " error:", err)
            else
                opt.incrby_script_sha = res
                incr_pre_val = 0
                release()
                ngx.log(ngx.DEBUG, "redis:script:incrby key:", incr_pre_key, " val:", incr_pre_val, " ttl:", ttl)
            end
        end
    end

    self.wcount = incr_pre_val + incr_val + (value or 1)
    self.cache:set(incr_key, self.wcount, 2*self.wind+1)
    ngx.log(ngx.DEBUG, "cache:set incr_key:", incr_key, " incr_val:", self.wcount)
end


local get_count_keys = function(self, key)
    local t = {}
    local day_sec = ceil((ngx.now() - anchor_ts) % 86400)
    local day_sec_index = floor(day_sec/self.wind)

    for i = 1, self.wnum do
        t[i] = key .. str_fmt("_%05d", day_sec_index)
        day_sec_index = day_sec_index - 1
        if day_sec_index < 0 then
            day_sec_index = self.midx
        end
    end

    return t
end


function _M.get(self, key)
    local k = tab_concat({self.name, key}, "_")
    local count = self.cache:get(k)
    ngx.log(ngx.DEBUG, "get cached key:", k, " val:", (count or "nil"))

    if count then
        return count + self.wcount
    end

    local redis, release = self.get_redis_conn_handler(k)
    if not redis then
        return self.wcount, release
    end

    local ks = get_count_keys(self, k)
    local res, err = redis:mget(unpack(ks))
    if not res then
        release(true)
        return self.wcount, err
    end

    count = 0
    for _, v in ipairs(res) do
        count = count + (tonumber(v) or 0)
    end

    self.cache:set(k, count + self.wcount, self.wind)
    ngx.log(ngx.DEBUG, "set cached key:", k, " val:", count + self.wcount)

    return count + self.wcount
end


return _M
