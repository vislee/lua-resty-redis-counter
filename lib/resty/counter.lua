-- Copyright (C) vislee

local redis = require "resty.redis"
local lrucache = require "resty.lrucache"

local ceil = math.ceil
local floor = math.floor
local str_fmt = string.format
local tab_concat = table.concat
local ngx_now = ngx.now
local ngx_crc32 = ngx.crc32_long

local _M = {cache = require "resty.cached" .new()}
local mt = { __index = _M }

local global = lrucache.new(32)
local anchor_ts = 1530460800

local redis_counter_incrbyex_script = [==[
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


local redis_counter_incrbyex = function(red, sha, key, val, ttl)
    if not sha then
        local res, err = red:script("LOAD", redis_counter_incrbyex_script)
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


local _redis_conn = function(opt)
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
_M.redis_conn = _redis_conn


local _get_hash_redis_conn = function(opts)
    return function(key)
        local idx = ngx_crc32(key) % #opts + 1
        return _redis_conn(opts[idx])
    end
end


function _M.new(name, wind, number, opts)
    local obj, stale_obj = global:get(name)
    if obj == nil and stale_obj then
        obj = stale_obj
    end
    if obj and obj.wind == wind and obj.wnum == number then
        return obj
    elseif obj then
        return nil, "wrong wind or number"
    end

    obj = setmetatable({
        name = name,
        wcount = {},
        wind = wind,
        midx = floor(86400/wind),
        wnum = number,
        redis_opts = opts,
        redis_conn_handler = _get_hash_redis_conn(opts),
    }, mt)
    global:set(name, obj, 3600)

    return obj
end


function _M.set_redis_conn_handler(self, get_x_redis_conn)
    self.redis_conn_handler = get_x_redis_conn(self.redis_opts)
end


function _M.close(self)
    global:delete(self.name)
    self.wcount = nil
end


function _M.incr(self, key, value)
    local day_sec = ceil((ngx.now() - anchor_ts) % 86400)
    local day_sec_index = floor(day_sec/self.wind)

    local incr_key = tab_concat({key, str_fmt("%05d", day_sec_index)}, '_')
    local incr_val, init = self.cache:incr(incr_key, (value or 1), 2*self.wind+1)
    ngx.log(ngx.DEBUG, "cache:incr incr_key:", incr_key, " incr_val:", incr_val)

    local incr_pre_key
    local incr_pre_val = 0

    -- change the count window
    if init then
        local day_sec_pre_index = day_sec_index - 1
        if day_sec_pre_index < 0 then
            day_sec_pre_index = self.midx
        end
        incr_pre_key = tab_concat({key, str_fmt("%05d", day_sec_pre_index)}, '_')
        incr_pre_val = self.cache:del(incr_pre_key)
        ngx.log(ngx.DEBUG, "cache:del incr_pre_key:", incr_pre_key, " incr_pre_val:", incr_pre_val)
    end

    -- incrby into redis
    if incr_pre_val > 0 then
        local redis, release, opt = self.redis_conn_handler(key)
        if redis then
            local ttl = self.wind * self.wnum + 3
            local res, err = redis_counter_incrbyex(redis, opt.incrby_script_sha, incr_pre_key, incr_pre_val, ttl)
            if not res then
                opt.incrby_script_sha = nil
                release(true)
                -- self.cache:incr(incr_key, ceil(incr_pre_val/3))
                ngx.log(ngx.WARN, "redis:script:incrby key:", incr_pre_key, " val:", incr_pre_val, " ttl:", ttl, " error:", err)
            else
                opt.incrby_script_sha = res
                release()
                ngx.log(ngx.DEBUG, "redis:script:incrby key:", incr_pre_key, " val:", incr_pre_val, " ttl:", ttl)
                incr_pre_val = 0
            end
        end
    end

    self.wcount[key] = incr_pre_val + incr_val
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
    local count = self.cache:get(key)
    ngx.log(ngx.DEBUG, "get cached key:", key, " val:", (count or "nil"))

    if count then
        return count + (self.wcount[key] or 0)
    end

    local redis, release = self.redis_conn_handler(key)
    if not redis then
        return (self.wcount[key] or 0), release
    end

    local ks = get_count_keys(self, key)
    local res, err = redis:mget(unpack(ks))
    if not res then
        release(true)
        return (self.wcount[key] or 0), err
    end
    release()

    count = 0
    for _, v in ipairs(res) do
        count = count + (tonumber(v) or 0)
    end

    self.cache:set(key, count + (self.wcount[key] or 0), self.wind)
    ngx.log(ngx.DEBUG, "set cached key:", key, " val:", count + (self.wcount[key] or 0))

    return count + (self.wcount[key] or 0)
end


return _M
