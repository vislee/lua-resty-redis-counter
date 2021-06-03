-- Copyright (C) vislee

local lrucache = require "resty.lrucache"
local redis = require "resty.redis"

local ceil = math.ceil
local floor = math.floor
local str_fmt = string.format

local _M = {}
local mt = { __index = _M }

local globle_cache = {}
local anchor_ts = 1530460800

local _get_redis_conn = function(opts)
    if opts == nil or type(opts) ~= "table" then
        return nil, "wrong opts"
    end

    if opts.host == nil then
        return nil, "opts.host is nil"
    end

    local red = redis:new()
    red:set_timeout(opts.timeout or 1000)
    local ok, err = red:connect(opts.host, (opts.port or 6379))
    if not ok then
        ngx.log(ngx.WARN, "failed to connect ", opts.host, ":", (opts.port or 6379), ". ", err)
        return nil, err
    end

    if opts.passwd and #opts.passwd > 0 then
        local res, err = red:auth(opts.passwd)
        if not res then
            return nil, err
        end
    end

    if opts.db and opts.db > 0 then
        red:select(opts.db)
    end

    return red, function()
        local ok, err = red:set_keepalive(10000, 32)
        if not ok then
            ngx.log(ngx.WARN, "failed to set keepalive: ", err)
            red:close()
        end
    end
end


function _M.new(name, wind, number, opts)
    local cache
    if name and ngx.shared[name] then
        cache = ngx.shared[name]
    elseif globle_cache[name] ~= nil then
        cache = globle_cache[name]
    else
        cache = lrucache.new(2048)
        globle_cache[name] = cache
    end

    local t = {
        name = name,
        wcount = 0,
        cache = cache,
        wind = wind,
        wnum = number,
        opts = opts,
        get_redis_conn = _get_redis_conn,
    }

    return setmetatable(t, mt)
end


function _M.flush(self)
    self.wcount = 0
    self.cache:flush_all()
    globle_cache[name] = nil
end


function _M.set_get_redis_conn(self, func)
    self.get_redis_conn = func
end


function _M.incr(self, key, val)
    local day_sec = ceil((ngx.now() - anchor_ts) % 86400)
    local day_sec_index = floor(day_sec/self.wind)

    local incr_key = key .. str_fmt("_%05d", day_sec_index)
    local incr_val = self.cache:get(incr_key) or 0
    ngx.log(ngx.DEBUG, "incr_key: ", incr_key, " incr_val: ", incr_val)

    local incr_pre_key
    local incr_pre_val = 0
    -- change the count window
    if incr_val == 0 then
        local day_sec_pre_index = day_sec_index - 1
        if day_sec_pre_index < 0 then
            day_sec_pre_index = floor(86400/self.wind)
        end
        incr_pre_key = key .. str_fmt("_%05d", day_sec_pre_index)
        incr_pre_val = self.cache:get(incr_pre_key) or 0
        ngx.log(ngx.DEBUG, "incr_pre_key: ", incr_pre_key, " incr_pre_val: ", incr_pre_val)
    end

    -- incrby into redis
    if incr_pre_val > 0 then
        local redis, release = self.get_redis_conn(self.opts)
        if redis then
            local val = redis:incrby(incr_pre_key, incr_pre_val)
            ngx.log(ngx.DEBUG, "redis:incrby key:", incr_pre_key, " val:", incr_pre_val, " return: ", val)
            local expire = self.wind * self.wnum + 3
            if incr_pre_val == val then
                redis:expire(incr_pre_key, expire)
                ngx.log(ngx.DEBUG, "expire key:", incr_pre_key, " ttl:", expire)
            end
            incr_pre_val = 0
            release()
        end
    end

    self.wcount = incr_pre_val + incr_val + (val or 1)
    self.cache:set(incr_key, self.wcount, 2*self.wind+1)
    ngx.log(ngx.DEBUG, "cache:set key:", incr_key, " val:", self.wcount)
end


local get_count_keys = function(self, key)
    local t = {}
    local day_sec = ceil((ngx.now() - anchor_ts) % 86400)
    local day_sec_index = floor(day_sec/self.wind)

    for i = 1, self.wnum do
        t[i] = key .. str_fmt("_%05d", day_sec_index)
        day_sec_index = day_sec_index - 1
        if day_sec_index < 0 then
            day_sec_index = floor(86400/self.wind)
        end
    end

    return t
end


function _M.get(self, key)
    local count = self.cache:get(key)
    ngx.log(ngx.DEBUG, "get cached key:", key, " val:", (count or ""))

    if count then
        return count + self.wcount
    end

    local redis, release = self.get_redis_conn(self.opts)
    if not redis then
        return 0, release
    end

    local ks = get_count_keys(self, key)
    local res, err = redis:mget(unpack(ks))
    if not res then
        return 0, err
    end

    count = 0
    for _, v in ipairs(res) do
        count = count + (tonumber(v) or 0)
    end

    self.cache:set(key, count, self.wind)
    return count + self.wcount
end


return _M
