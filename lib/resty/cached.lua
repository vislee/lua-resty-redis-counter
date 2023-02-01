-- Copyright (C) vislee

local lrucache = require "resty.lrucache"
local resty_lock = require "resty.lock"

local _M = {}
local mt = { __index = _M }

local share_name = "counter"
local lock_opts = {
    exptime = 0.02,
    timeout = 0.01,
    step = 0.001,
    max_step = 0.1,
}


function _M.new()
    local cache, shared

    if ngx.shared[share_name] then
        cache = ngx.shared[share_name]
        shared = true
    else
        cache = lrucache.new(4096)
        shared = false
    end

    return setmetatable({cache = cache, shared = shared}, mt)
end


function _M.set(self, key, val, ttl, flag)
    self.cache:set(key, val, ttl, flag)
end


-- return newval, isinit
function _M.incr(self, key, val, ttl)
    local newval, init = 0, false

    if self.shared then
        newval = self.cache:incr(key, val, 0, ttl)
        if newval == val then
            init = true
        end
        return newval, init
    end

    local data = self.cache:get(key)
    if not data then
        init = true
        data = 0
    end
    newval = val + data

    self.cache:set(key, newval, ttl)

    return newval, init
end


-- return val, flag
function _M.get(self, key)
    if self.shared then
        return self.cache:get(key)
    end

    local val, _, flag = self.cache:get(key)
    return val, flag
end


-- return oldval
function _M.del(self, key)
    if self.shared then
        local lock = resty_lock:new(share_name, lock_opts)
        local elapsed
        if lock then
            elapsed = lock:lock("lock_" .. key)
        end

        local val = self.cache:get(key)
        if val then
            self.cache:delete(key)
        end

        if elapsed then
            lock:unlock()
        end

        return val or 0
    end

    local val = self.cache:get(key) or 0
    if val > 0 then
        self.cache:delete(key)
    end

    return val
end


function _M.flush_all(self)
    self.cache:flush_all()
end


return _M
